use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use moka::sync::Cache as MokaCache;
use tokio_tungstenite::tungstenite::Error;

use crate::types::{
    Account, AccountKey, AccountWithKey, CacheHitStatus, Commitment, ProgramAccounts, ProgramKey,
};
use crate::ws::manager::WsConnectionManager;
use crate::ws::notification::ProgramNotification;
use crate::ws::subscription::SubscriptionInfo;
use crate::{metrics::METRICS, types::OptionalAccount};

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
    accounts: MokaCache<AccountKey, OptionalAccount>,
    programs: MokaCache<ProgramKey, Arc<ProgramAccounts>>,
    slots: [Arc<AtomicU64>; 3],
}

/// Main type for using library, keeps account and program entries, along with
/// managing their updates via websocket updates, and tracks TTL of each entry
pub struct Cache {
    inner: InnerCache,
    ws: WsConnectionManager,
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

impl InnerCache {
    fn new(accounts: u64, programs: u64) -> Self {
        let accounts = MokaCache::new(accounts);
        let programs = MokaCache::new(programs);
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
    pub(crate) fn remove_account(&self, key: &AccountKey) {
        self.accounts.invalidate(key)
    }

    #[inline]
    pub(crate) fn contains_account(&self, key: &AccountKey) -> bool {
        self.accounts.contains_key(key)
    }

    #[inline]
    pub(crate) fn contains_program(&self, key: &ProgramKey) -> bool {
        self.programs.contains_key(key)
    }

    #[inline]
    pub(crate) fn remove_program(&self, key: &ProgramKey) {
        let keys = match self.programs.get(key) {
            Some(program) => {
                self.programs.invalidate(key);
                program
            }
            None => return,
        };
        let commitment = key.commitment;
        for &pubkey in keys.iter() {
            let key = AccountKey { pubkey, commitment };
            self.accounts.invalidate(&key);
        }
    }

    #[inline]
    pub(crate) fn update_account(&self, key: AccountKey, account: Account) -> bool {
        if self.accounts.contains_key(&key) {
            return false;
        }
        self.accounts.insert(key, Some(account).into());
        true
    }

    #[inline]
    pub(crate) fn upsert_program_account(
        &mut self,
        notification: ProgramNotification,
        programkey: &ProgramKey,
    ) -> bool {
        if let Some(mut keys) = self.programs.get(programkey) {
            if !keys.contains(&notification.pubkey) {
                keys = Arc::new(keys.clone_with_key(notification.pubkey));
                self.programs.insert(programkey.clone(), keys);
            }
        } else {
            return false;
        }
        let key = AccountKey {
            pubkey: notification.pubkey,
            commitment: programkey.commitment,
        };
        let account = Some(Account::from(notification.account));
        self.accounts.insert(key, account.into());
        true
    }
}

impl Cache {
    /// Construct new cache instance. One instance should be cloned and shared
    /// between multiple threads, instead of creating more new instances
    pub async fn new(
        accounts_capacity: u64,
        programs_capacity: u64,
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
    pub async fn store_account<O: Into<OptionalAccount>>(&self, key: AccountKey, account: O) {
        if self.inner.accounts.contains_key(&key) {
            return;
        }
        self.inner.accounts.insert(key.clone(), account.into());
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
        if self.inner.programs.contains_key(&key) {
            return;
        }
        let mut program_accounts = Vec::new();
        for a in accounts {
            let record: CacheableAccount = a.into();
            program_accounts.push(record.key.pubkey);
            self.inner
                .accounts
                .insert(record.key, Some(record.account).into());
        }
        let program_accounts = Arc::new(ProgramAccounts::new(program_accounts));
        self.inner.programs.insert(key.clone(), program_accounts);
        let info = SubscriptionInfo::Program(Box::new(key));
        self.ws.subscribe(info).await;
    }

    /// Retrieve account information from cache for the given key,
    /// will reset TTL for the given account entry in cache
    pub fn get_account(&self, key: &AccountKey) -> CacheHitStatus<Arc<Account>> {
        let value = match self.inner.accounts.get(key) {
            Some(v) => v,
            None => return CacheHitStatus::Miss,
        };
        match value {
            OptionalAccount::Exists(ref acc) => CacheHitStatus::Hit(Arc::clone(acc)),
            OptionalAccount::Absent => CacheHitStatus::HitButEmpty,
        }
    }

    /// Retrieve program accounts from cache for the given program key,
    /// will reset TTL for the given program entry in cache
    pub fn get_program_accounts(&self, key: &ProgramKey) -> Option<Vec<AccountWithKey>> {
        let keys = self.inner.programs.get(key)?;
        let mut result = Vec::with_capacity(keys.len());
        let mut corrupted = false;
        // collect program accounts from accounts cache
        for &pubkey in keys.iter() {
            let acckey = AccountKey {
                pubkey,
                commitment: key.commitment,
            };
            let account = match self.inner.accounts.get(&acckey) {
                Some(a) => a,
                None => {
                    tracing::warn!("no account is present in cache, while program has its key");
                    corrupted = true;
                    break;
                }
            };
            let account = match account {
                OptionalAccount::Exists(ref acc) => Arc::clone(acc),
                OptionalAccount::Absent => {
                    tracing::warn!(
                        "account entry is present in cache, but has no data, shouldn't happen for program"
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
        // and its accounts. This situation is however should rarely if ever happen
        if corrupted {
            METRICS.corrupted_programs.inc();
            self.inner.programs.invalidate(key);
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
