use std::hash::Hash;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use dashmap::mapref::one::{Ref, RefMut};
use dashmap::DashMap;
use lru::LruCache;
use tokio_tungstenite::tungstenite::Error;

use crate::metrics::METRICS;
use crate::types::{
    Account, AccountKey, AccountWithKey, CacheHitStatus, CacheValue, Commitment, ProgramAccounts,
    ProgramKey, Record,
};
use crate::ws::manager::WsConnectionManager;
use crate::ws::notification::ProgramNotification;
use crate::ws::subscription::{SubMeta, SubscriptionInfo};

struct Lru<K, V> {
    storage: Arc<DashMap<K, CacheValue<V>>>,
    evictor: Arc<Mutex<LruCache<K, ()>>>,
}

/// Proxy type for converting account
/// information from external sources
pub struct CacheableAccount {
    /// Cache key, by which this account should be identified
    pub key: AccountKey,
    /// Account information
    pub account: Account,
}

/// Core cache structure, for storing account/program/slot data
pub(crate) struct InnerCache {
    accounts: Lru<AccountKey, Option<Arc<Account>>>,
    programs: Lru<ProgramKey, ProgramAccounts>,
    slots: [Arc<AtomicU64>; 3],
}

/// Main type for using library, keeps account and program entries, along with
/// managing their updates via websocket updates, and tracks TTL of each entry
pub struct Cache {
    inner: InnerCache,
    ws: WsConnectionManager,
}

struct InsertionResult<V> {
    previous: Option<CacheValue<V>>,
    evicted: Option<CacheValue<V>>,
}

impl<K, V> Clone for Lru<K, V> {
    fn clone(&self) -> Self {
        let storage = Arc::clone(&self.storage);
        let evictor = Arc::clone(&self.evictor);
        Self { storage, evictor }
    }
}

impl<K: Eq + Hash + Clone, V: Record> Lru<K, V> {
    fn new(capacity: usize) -> Self {
        let storage = Arc::new(DashMap::with_capacity(capacity));
        let evictor = Arc::new(Mutex::new(LruCache::new(capacity)));
        Self { storage, evictor }
    }

    /// create new entry in cache, if key exists, update
    /// it and return true, otherwise false is returned
    #[inline]
    fn insert(&self, key: K, value: V, exempt: bool) -> InsertionResult<V> {
        let mut size: isize;
        let previous = {
            let key = key.clone();
            let value = CacheValue::new(value);
            size = value.size() as isize;
            self.storage.insert(key, value)
        };
        let mut evicted = None;
        let mut evictor = self
            .evictor
            .lock()
            .expect("cache evictor mutex is poisoned");
        // if the value already existed in cache, and request was made to make it eviction exempt,
        // then we should remove it from evictor, but leave it in main storage
        if previous.is_some() && exempt {
            evictor.pop(&key);
        } else if !exempt {
            if let Some((k, _)) = evictor.push(key, ()) {
                evicted = self.storage.remove(&k).map(|(_, v)| v);
            }
        };
        if let Some(ref v) = evicted {
            size -= v.size() as isize;
            V::eviction_metrics().inc();
        }
        V::size_metrics().add(size as i64);
        V::count_metrics().set(self.storage.len() as i64);

        InsertionResult { previous, evicted }
    }

    #[inline]
    fn update(&self, key: &K, value: V) {
        let mut size = value.size() as isize;
        if let Some(mut v) = self.storage.get_mut(key) {
            size -= v.set_value(value).size() as isize;
        }
        V::size_metrics().add(size as i64);
    }

    fn will_exceed_capacity(&self, extra: usize) -> bool {
        self.storage.len() + extra > self.storage.capacity()
    }

    fn evict(&self) -> Option<CacheValue<V>> {
        let (key, _) = self
            .evictor
            .lock()
            .expect("evictor mutex is poisoned")
            .pop_lru()?;
        self.storage.remove(&key).map(|(_, v)| v)
    }

    #[inline]
    fn get(&self, key: &K, touch: bool) -> Option<Ref<'_, K, CacheValue<V>>> {
        if !self.storage.contains_key(key) {
            return None;
        }
        if touch {
            self.evictor
                .lock()
                .expect("cache evictor mutex is poisoned")
                .get(key);
        }
        self.storage.get(key)
    }

    #[inline]
    fn get_mut(&self, key: &K) -> Option<RefMut<'_, K, CacheValue<V>>> {
        self.storage.get_mut(key)
    }

    #[inline]
    fn contains(&self, key: &K) -> bool {
        self.storage.contains_key(key)
    }

    fn set_meta(&self, key: &K, meta: SubMeta) {
        if let Some(mut value) = self.storage.get_mut(key) {
            value.set_subscription(meta);
        }
    }

    pub fn remove(&self, key: &K, cleanup: bool) -> Option<CacheValue<V>> {
        let (key, value) = self.storage.remove(key)?;
        if cleanup {
            self.evictor
                .lock()
                .expect("cache evictor mutex is poisoned")
                .pop(&key);
        }
        V::size_metrics().sub(value.size() as i64);
        V::count_metrics().set(self.storage.len() as i64);
        Some(value)
    }
}

impl Clone for InnerCache {
    fn clone(&self) -> Self {
        let accounts = self.accounts.clone();
        let programs = self.programs.clone();
        let slots = self.slots.clone();
        Self {
            accounts,
            programs,
            slots,
        }
    }
}

impl Clone for Cache {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        let ws = self.ws.clone();
        Self { inner, ws }
    }
}

impl<T> CacheValue<T> {
    fn new(value: T) -> Self {
        Self { value, sub: None }
    }

    pub fn set_subscription(&mut self, sub: SubMeta) {
        self.sub.replace(sub);
    }

    #[inline]
    pub fn set_value(&mut self, value: T) -> T {
        std::mem::replace(&mut self.value, value)
    }
}

impl InnerCache {
    fn new(accounts: usize, programs: usize) -> Self {
        let accounts = Lru::new(accounts);
        let programs = Lru::new(programs);
        let slots = [Arc::default(), Arc::default(), Arc::default()];
        Self {
            accounts,
            programs,
            slots,
        }
    }
    #[inline]
    pub(crate) fn update_slot(&self, slot: u64, commitment: Commitment) {
        self.slots[commitment as usize].store(slot, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn remove_account(
        &self,
        key: &AccountKey,
    ) -> Option<CacheValue<Option<Arc<Account>>>> {
        self.accounts.remove(key, true)
    }

    #[inline]
    pub(crate) fn remove_program(&self, key: &ProgramKey) {
        let keys = match self.programs.remove(key, true) {
            Some(program) => program.value,
            None => return,
        };
        let commitment = key.commitment;
        for pubkey in keys {
            let key = AccountKey { pubkey, commitment };
            self.accounts.remove(&key, false);
        }
    }

    #[inline]
    pub(crate) fn update_account_meta(&self, key: &AccountKey, meta: SubMeta) {
        self.accounts.set_meta(key, meta);
    }

    #[inline]
    pub(crate) fn update_program_meta(&self, key: &ProgramKey, meta: SubMeta) {
        self.programs.set_meta(key, meta);
    }

    #[inline]
    pub(crate) fn update_account(&self, key: &AccountKey, account: Account) {
        self.accounts.update(key, Some(Arc::new(account)));
    }

    #[inline]
    pub(crate) fn upsert_program_account(
        &mut self,
        notification: ProgramNotification,
        programkey: &ProgramKey,
    ) {
        let account = Some(Arc::new(notification.account.into()));
        if let Some(mut entry) = self.programs.get_mut(programkey) {
            let keys = &mut entry.value_mut().value;
            keys.insert(notification.pubkey);
        } else {
            return;
        }
        let key = AccountKey {
            pubkey: notification.pubkey,
            commitment: programkey.commitment,
        };
        self.accounts.insert(key, account, true);
    }
}

impl Cache {
    /// Construct new cache instance. One instance should be cloned and shared
    /// between multiple threads, instead of creating more new instances
    pub async fn new(
        accounts_capacity: usize,
        programs_capacity: usize,
        ws_url: String,
        connection_count: usize,
    ) -> Result<Self, Error> {
        let inner = InnerCache::new(accounts_capacity, programs_capacity);
        let ws = WsConnectionManager::new(ws_url, connection_count, inner.clone()).await?;
        let cache = Self { inner, ws };
        Ok(cache)
    }

    /// Store account information in cache, and subscribe to updates of the given account via
    /// websocket subscriptions, might cause eviction of older values if cache is full
    pub async fn store_account(&self, key: AccountKey, account: Option<Account>) {
        if self.inner.accounts.contains(&key) {
            return;
        }
        let account = account.map(Arc::new);
        let result = self.inner.accounts.insert(key.clone(), account, false);
        // check for eviction and unsubscribe if necessary
        if let Some(v) = result.evicted {
            if let Some(meta) = v.sub {
                self.ws.unsubscribe(meta).await;
            }
        }
        let info = SubscriptionInfo::Account(key);
        self.ws.subscribe(info).await;
    }

    /// Store program accounts in cache, making them eviction exempt (as to not break consistency).
    /// Subscribe to updates of the given program's accounts (satisfying filters) via websocket
    /// subscriptions, while unsubscribing from account subscriptions, if they existed in cache
    /// before program insertion
    pub async fn store_program<C: Into<CacheableAccount>>(
        &self,
        key: ProgramKey,
        accounts: Vec<C>,
    ) {
        if self.inner.programs.contains(&key) {
            return;
        }
        let mut to_unsubscribe = Vec::new();
        let mut program_accounts = Vec::new();
        // if the insertion of program will go over capacity of accounts, remove lru program to
        // make space
        if self.inner.accounts.will_exceed_capacity(accounts.len()) {
            if let Some(v) = self.inner.programs.evict() {
                let commitment = key.commitment;
                for pubkey in v.value {
                    let key = AccountKey { pubkey, commitment };
                    self.inner.accounts.remove(&key, false);
                }
                if let Some(meta) = v.sub {
                    self.ws.unsubscribe(meta).await;
                }
            }
        }
        for a in accounts {
            let record: CacheableAccount = a.into();
            program_accounts.push(record.key.pubkey);
            let result =
                self.inner
                    .accounts
                    .insert(record.key, Some(Arc::new(record.account)), true);
            if let Some(entry) = result.previous {
                if let Some(meta) = entry.sub {
                    to_unsubscribe.push(meta);
                }
            }
        }
        let program_accounts = ProgramAccounts::new(program_accounts);
        let result = self
            .inner
            .programs
            .insert(key.clone(), program_accounts, false);
        // if we run out of cache capacity and some program was evicted from cache, we need to
        // unsubscribe from it, and also remove its accounts as they are not subject to eviction
        if let Some(v) = result.evicted {
            if let Some(meta) = v.sub {
                self.ws.unsubscribe(meta).await;
            }
            let commitment = key.commitment;
            for pubkey in v.value {
                let key = AccountKey { pubkey, commitment };
                self.inner.accounts.remove(&key, false);
            }
        }
        let info = SubscriptionInfo::Program(Box::new(key));
        self.ws.subscribe(info).await;
        for m in to_unsubscribe {
            self.ws.unsubscribe(m).await;
        }
    }

    /// Retrieve account information from cache for the given key,
    /// will reset TTL for the given account entry in cache
    pub fn get_account(&self, key: &AccountKey) -> CacheHitStatus<Arc<Account>> {
        // get account from cache and "touch" it, so that it will not be subject to eviction in
        // near future
        let entry = match self.inner.accounts.get(key, true) {
            Some(v) => v,
            None => return CacheHitStatus::Miss,
        };
        let value = &entry.value().value;
        match value {
            Some(acc) => CacheHitStatus::Hit(Arc::clone(acc)),
            None => CacheHitStatus::HitButEmpty,
        }
    }

    /// Retrieve program accounts from cache for the given program key,
    /// will reset TTL for the given program entry in cache
    pub async fn get_program_accounts(&self, key: &ProgramKey) -> Option<Vec<AccountWithKey>> {
        let keys = {
            let entry = self.inner.programs.get(key, true)?;
            entry.value().value.clone()
        };
        let mut result = Vec::with_capacity(keys.len());
        let mut corrupted = false;
        // collect program accounts from accounts cache
        for pubkey in keys {
            let acckey = AccountKey {
                pubkey,
                commitment: key.commitment,
            };
            // don't "touch" those accounts as they are eviction exempt and this operation will
            // unnecessarily incur locking of evictor's mutex
            let entry = match self.inner.accounts.get(&acckey, false) {
                Some(e) => e,
                None => {
                    tracing::warn!("no account is present in cache, while program has its key");
                    corrupted = true;
                    break;
                }
            };
            let account = match &entry.value().value {
                Some(acc) => Arc::clone(acc),
                None => {
                    tracing::warn!(
                        "account entry is present in cache, but has no data, shoulnd't happen for program"
                    );
                    corrupted = true;
                    break;
                }
            };
            let acc = AccountWithKey { pubkey, account };
            result.push(acc);
        }
        // If for some reason one of program's accounts was deleted (e.g. two programs referencing
        // same account), then the cache state is inconsistent, and we should cleanup the program
        // and its accounts. This situation is however should raraly if at all happen
        if corrupted {
            METRICS.corrupted_programs.inc();
            let entry = self.inner.programs.remove(key, true)?;
            let commitment = key.commitment;
            for pubkey in entry.value {
                let key = AccountKey { pubkey, commitment };
                self.inner.accounts.remove(&key, false);
            }
            if let Some(meta) = entry.sub {
                self.ws.unsubscribe(meta).await;
            }
            return None;
        }

        Some(result)
    }

    /// Load latest slot number for given commitment level
    pub fn get_slot<C: Into<Commitment>>(&self, commitment: C) -> u64 {
        self.inner.slots[commitment.into() as usize].load(Ordering::Relaxed)
    }
}

// =======================================================
// ======================= TESTS =========================
// =======================================================

//#[cfg(test)]
//mod tests {
//#![allow(unused)]

//use std::{sync::Arc, time::Duration};

//use crate::{
//cache::CacheableAccount,
//types::{
//Account, AccountKey, AccountWithKey, CacheHitStatus, CachedPubkey, Commitment,
//ProgramKey,
//},
//ws::notification::AccountNotification,
//};

//use super::Cache;

//const WS_URL_ENV_VAR: &str = "CACHEIUM_WS_URL";

//async fn init_cache() -> Cache {
//let url = std::env::var(WS_URL_ENV_VAR).expect("CACHEIUM_WS_URL env variable is not set");
//let cache = match Cache::new(url, 1, Duration::from_secs(2)).await {
//Ok(cache) => cache,
//Err(err) => panic!("cache creation error: {}", err),
//};
//cache
//}

//#[tokio::test]
//async fn test_cache_init() {
//init_cache().await;
//}

//#[test]
//async fn test_cache_account() {
//let cache = init_cache().await;
//let account = Account {
//owner: CachedPubkey::new([0; 32]),
//data: vec![0; 32],
//executable: false,
//lamports: 32,
//rent_epoch: 3234,
//};

//let key = AccountKey {
//pubkey: CachedPubkey::new([1; 32]),
//commitment: Commitment::Confirmed,
//};
//cache.store_account(key.clone(), Some(account.clone()));
//let acc = cache.get_account(&key);
//assert_eq!(acc, CacheHitStatus::Hit(Arc::new(account)));
//let acc = cache.get_account(&key);
//assert_eq!(acc, CacheHitStatus::Miss);
//}

//#[tokio::test]
//async fn test_cache_program() {
//let cache = init_cache().await;
//let mut accounts = Vec::with_capacity(100);
//for i in 0..100 {
//let account = Account {
//owner: CachedPubkey::new([i; 32]),
//data: vec![1; (i * 2) as usize],
//executable: false,
//lamports: i as u64,
//rent_epoch: i as u64,
//};
//let cachable = CacheableAccount {
//key: AccountKey {
//pubkey: CachedPubkey::new([i + 1; 32]),
//commitment: Commitment::Processed,
//},
//account,
//};
//accounts.push(cachable);
//}
//let key = ProgramKey {
//pubkey: CachedPubkey::new([0; 32]),
//commitment: Commitment::Processed,
//filters: None,
//};
//cache.store_program(key.clone(), accounts).await;
//let accounts = cache.get_program_accounts(&key).await;
//assert!(accounts.is_some());
//println!("ACC: {:?}", accounts);
//assert_eq!(accounts.unwrap().len(), 100);
//tokio::time::sleep(Duration::from_secs(3)).await;
//let accounts = cache.get_program_accounts(&key).await;
//assert_eq!(accounts, None);
//}
//}
