use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use dashmap::DashMap;
use futures_delay_queue::{delay_queue, DelayHandle, DelayQueue as FutureDelayQueue, Receiver};
use futures_intrusive::buffer::GrowingHeapBuf;
use tokio::select;
use tokio_tungstenite::tungstenite::Error;

use crate::{
    types::{Account, AccountKey, AccountWithKey, Commitment, ProgramAccounts, ProgramKey},
    ws::manager::WsConnectionManager,
    ws::notification::ProgramNotification,
    ws::subscription::{SubMeta, SubscriptionInfo},
};

type DelayQueue<T> = Arc<Mutex<FutureDelayQueue<T, GrowingHeapBuf<T>>>>;

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
    accounts: Arc<DashMap<AccountKey, CacheValue<Arc<Account>>>>,
    programs: Arc<DashMap<ProgramKey, CacheValue<ProgramAccounts>>>,
    slots: [Arc<AtomicU64>; 3],
}

/// Main type for using library, keeps account and program entries, along with
/// managing their updates via websocket updates, and tracks TTL of each entry
pub struct Cache {
    inner: InnerCache,
    arx: Receiver<AccountKey>,
    prx: Receiver<ProgramKey>,
    aqueue: DelayQueue<AccountKey>,
    pqueue: DelayQueue<ProgramKey>,
    ws: WsConnectionManager,
    ttl: Duration,
}

pub(crate) struct CacheValue<T> {
    pub value: T,
    pub handle: Option<DelayHandle>,
    pub sub: Option<SubMeta>,
    // External reference to account entry, used by programs
    // Ordinary usize, as dashmap locks entire entry before
    // giving out references to it, so race is impossible
    pub refs: usize,
}

impl Clone for InnerCache {
    fn clone(&self) -> Self {
        let accounts = Arc::clone(&self.accounts);
        let programs = Arc::clone(&self.programs);
        let slots = self.slots.clone();
        Self {
            accounts,
            programs,
            slots,
        }
    }
}

impl Default for InnerCache {
    fn default() -> Self {
        let accounts = Arc::default();
        let programs = Arc::default();
        let slots = [Arc::default(), Arc::default(), Arc::default()];
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
        let arx = self.arx.clone();
        let prx = self.prx.clone();
        let aqueue = Arc::clone(&self.aqueue);
        let pqueue = Arc::clone(&self.pqueue);
        let ws = self.ws.clone();
        let ttl = self.ttl;
        Self {
            inner,
            arx,
            prx,
            aqueue,
            pqueue,
            ws,
            ttl,
        }
    }
}

impl<T> CacheValue<T> {
    fn new(value: T, handle: Option<DelayHandle>) -> Self {
        Self {
            value,
            handle,
            sub: None,
            refs: 0,
        }
    }

    pub fn set_subscription(&mut self, sub: SubMeta) {
        self.sub.replace(sub);
    }

    #[inline]
    pub fn set_value(&mut self, value: T) {
        self.value = value;
    }

    #[inline]
    pub fn set_delay(&mut self, delay: DelayHandle) {
        self.handle.replace(delay);
    }
}

impl InnerCache {
    #[inline]
    pub(crate) fn update_slot(&self, slot: u64, commitment: Commitment) {
        self.slots[commitment as usize].store(slot, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn remove_account(&self, key: &AccountKey) {
        self.accounts.remove(key);
    }

    #[inline]
    pub(crate) fn remove_program(&self, key: &ProgramKey) {
        self.programs.remove(key);
    }

    #[inline]
    pub(crate) fn update_account_meta(&self, key: &AccountKey, meta: SubMeta) {
        if let Some(mut v) = self.accounts.get_mut(key) {
            v.set_subscription(meta);
        }
    }

    #[inline]
    pub(crate) fn update_program_meta(&self, key: &ProgramKey, meta: SubMeta) {
        if let Some(mut v) = self.programs.get_mut(key) {
            v.set_subscription(meta);
        }
    }

    #[inline]
    pub(crate) fn update_account(&self, key: &AccountKey, account: Account) {
        if let Some(mut v) = self.accounts.get_mut(key) {
            v.set_value(Arc::new(account));
        }
    }

    #[inline]
    pub(crate) fn upsert_program_account(
        &mut self,
        notification: ProgramNotification,
        programkey: &ProgramKey,
    ) {
        let account = Arc::new(notification.account.into());
        if let Some(mut v) = self.programs.get_mut(programkey) {
            let acc = AccountWithKey {
                pubkey: notification.pubkey,
                account: Arc::clone(&account),
            };
            v.value.accounts_mut().insert(acc);
        } else {
            return;
        }
        let key = AccountKey {
            pubkey: notification.pubkey,
            commitment: programkey.commitment,
        };
        if let Some(mut v) = self.accounts.get_mut(&key) {
            v.set_value(account);
        } else {
            let value = CacheValue::new(account, None);
            self.accounts.insert(key, value);
        };
    }
}

impl Cache {
    /// Construct new cache instance. One instance should cloned and shared
    /// between multiple threads, instead of creating more new instances
    pub async fn new(
        ws_url: String,
        connection_count: usize,
        ttl: Duration,
    ) -> Result<Self, Error> {
        let inner = InnerCache::default();
        let (aqueue, arx) = delay_queue();
        let (pqueue, prx) = delay_queue();
        let ws = WsConnectionManager::new(ws_url, connection_count, inner.clone()).await?;
        let cache = Self {
            inner,
            arx,
            prx,
            aqueue: Arc::new(Mutex::new(aqueue)),
            pqueue: Arc::new(Mutex::new(pqueue)),
            ws,
            ttl,
        };
        let clone = cache.clone();
        // start separate task to manage ttl expirations and corresponding cleanups
        tokio::spawn(clone.cleanup());
        Ok(cache)
    }

    /// Store account information in cache, set its ttl, and subscribe
    /// to updates of the given account via websocket subscriptions
    pub async fn store_account<C: Into<CacheableAccount>>(&self, record: C) {
        let record = record.into();
        if self.inner.accounts.contains_key(&record.key) {
            // it's possible that concurrent requests were made for the
            // same account, so prevent them from storing the same data
            return;
        };
        let delay = self
            .aqueue
            .lock()
            .unwrap()
            .insert(record.key.clone(), self.ttl);

        let value = CacheValue::new(Arc::new(record.account), Some(delay));
        self.inner.accounts.insert(record.key.clone(), value);
        let info = SubscriptionInfo::Account(record.key);
        self.ws.subscribe(info).await;
    }

    /// Store program accounts in cache, set ttl for entire program entry, and cancel ttls of
    /// account entries if those existed before. Subscribe to updates of the given program account
    /// (satisfying filters) via websocket subscriptions, while unsubscribing from account
    /// subscriptions, if they existed in cache before program insertion
    pub async fn store_program<C: Into<CacheableAccount>>(
        &self,
        key: ProgramKey,
        accounts: Vec<C>,
    ) {
        let mut to_unsubscribe = Vec::new();
        let mut program_accounts = Vec::new();
        for a in accounts {
            let record: CacheableAccount = a.into();
            // if account entry already exists in cache, leave it
            if let Some(mut entry) = self.inner.accounts.get_mut(&record.key) {
                // but increment reference count to it, so that ttl based garbage
                // collection won't clean it up while program still references it
                entry.refs += 1;
                let account_with_key = AccountWithKey {
                    pubkey: record.key.pubkey,
                    account: Arc::clone(&entry.value),
                };
                // and prepare to unsubscribe from account, if it has active subscription, as program
                // subscription will manage updates from now on
                if let Some(meta) = entry.sub.take() {
                    to_unsubscribe.push(meta);
                }
                // and cancel ttl for the given account, its now
                // program's responsibility to clean it up
                if let Some(handle) = entry.handle.take() {
                    // shouldn't hold reference to dashmap while making
                    // async call lest it might deadlock
                    drop(entry);
                    let _ = handle.cancel().await;
                }
                program_accounts.push(account_with_key);
            } else {
                let account = Arc::new(record.account);
                let mut value = CacheValue::new(Arc::clone(&account), None);
                // set reference count to 1
                value.refs += 1;
                let account_with_key = AccountWithKey {
                    pubkey: record.key.pubkey,
                    account,
                };
                self.inner.accounts.insert(record.key, value);
                program_accounts.push(account_with_key);
            }
        }
        let delay = self.pqueue.lock().unwrap().insert(key.clone(), self.ttl);
        let value = CacheValue::new(ProgramAccounts::new(program_accounts), Some(delay));
        self.inner.programs.insert(key.clone(), value);
        let info = SubscriptionInfo::Program(Box::new(key));
        self.ws.subscribe(info).await;
        for m in to_unsubscribe {
            self.ws.unsubscribe(m).await;
        }
    }

    /// Retrieve account information from cache for the given key,
    /// will reset TTL for the given account entry in cache
    pub async fn get_account(&self, key: &AccountKey) -> Option<Arc<Account>> {
        let mut res = self.inner.accounts.get_mut(key)?;
        let retval = Some(Arc::clone(&res.value));
        if let Some(handle) = res.handle.take() {
            // to avoid deadlock, drop reference to entry, as holding the reference
            // to dashmap entry across an await point will most likely deadlock
            drop(res);
            let handle = handle.reset(self.ttl).await.ok()?; // return None if already expired
            if let Some(mut entry) = self.inner.accounts.get_mut(key) {
                // check the handle, just in case if other request
                // has already set it during an await above
                if entry.handle.is_none() {
                    entry.handle.replace(handle);
                }
            }
        } else {
            // if ttl doesn't exist, then this account is definetly managed by program, by creating
            // ttl handle, and incrementing reference count, we register interest in given account
            // entry, and it won't get cleared after program's ttl expires
            let delay = self.aqueue.lock().unwrap().insert(key.clone(), self.ttl);
            res.set_delay(delay);
        }
        retval
    }

    /// Retrieve program accounts from cache for the given program key,
    /// will reset TTL for the given program entry in cache
    pub async fn get_program_accounts(&self, key: &ProgramKey) -> Option<Vec<AccountWithKey>> {
        let mut res = self.inner.programs.get_mut(key)?;
        let accounts = res.value.accounts().iter().cloned().collect();
        if let Some(handle) = res.handle.take() {
            // drop reference to avoid potential deadlock across an await point
            drop(res);
            let handle = handle.reset(self.ttl).await.ok()?; // if expired return none
            self.inner.programs.get_mut(key)?.handle.replace(handle);
        } // else program accounts always have ttl handle

        Some(accounts)
    }

    /// Load latest slot number for given commitment level
    pub fn get_slot<C: Into<Commitment>>(&self, commitment: C) -> u64 {
        self.inner.slots[commitment.into() as usize].load(Ordering::Relaxed)
    }

    async fn remove_account(&self, key: AccountKey) {
        if let Some(mut acc) = self.inner.accounts.get_mut(&key) {
            // remove ttl handle (guaranteed to be expired) as this will allow
            // program to cleanup the account later, if this method fails to do so
            acc.handle.take();
        }
        // Only remove account if it's not referenced by any other program
        let sub = self
            .inner
            .accounts
            .remove_if(&key, |_, v| v.refs == 0)
            .and_then(|(_, v)| v.sub);

        if let Some(meta) = sub {
            self.ws.unsubscribe(meta).await;
        }
    }

    async fn remove_program(&self, key: ProgramKey) {
        if let Some((_, v)) = self.inner.programs.remove(&key) {
            let keys = v.value.into_iter();
            let commitment = key.commitment;
            for pubkey in keys.map(|acc| acc.pubkey) {
                let key = AccountKey { pubkey, commitment };
                if let Some(mut acc) = self.inner.accounts.get_mut(&key) {
                    acc.refs -= 1;
                }

                let mut resubscribe = false;
                // only remove accounts, if no other programs reference them, and no separate
                // account request was recently made to it (absence of ttl handle)
                self.inner.accounts.remove_if(&key, |_, v| {
                    if v.handle.is_some() {
                        // if ttl handle exists, then we should subscribe for account updates,
                        // but only if no other program is managing its ws subscription
                        resubscribe = v.refs == 0;
                        return false;
                    }
                    v.refs == 0
                });
                if resubscribe {
                    let info = SubscriptionInfo::Account(key);
                    self.ws.subscribe(info).await;
                }
            }
            match v.sub {
                Some(meta) => self.ws.unsubscribe(meta).await,
                None => tracing::warn!("no subscription meta is found for cache entry"),
            }
        }
    }

    /// Checks for expired ttls and performs cache cleanup
    async fn cleanup(self) {
        loop {
            select! {
                biased; Some(programkey) = self.prx.receive() => {
                    self.remove_program(programkey).await;
                }
                Some(accountkey) = self.arx.receive() => {
                    self.remove_account(accountkey).await;
                },
            }
        }
    }
}

// =======================================================
// ======================= TESTS =========================
// =======================================================

#[cfg(test)]
mod tests {
    #![allow(unused)]

    use std::{sync::Arc, time::Duration};

    use bytes::Bytes;

    use crate::{
        cache::CacheableAccount,
        types::{Account, AccountKey, AccountWithKey, CachedPubkey, Commitment, ProgramKey},
        ws::notification::AccountNotification,
    };

    use super::Cache;

    const WS_URL_ENV_VAR: &str = "CACHEIUM_WS_URL";

    async fn init_cache() -> Cache {
        let url = std::env::var(WS_URL_ENV_VAR).expect("CACHEIUM_WS_URL env variable is not set");
        let cache = match Cache::new(url, 1, Duration::from_secs(2)).await {
            Ok(cache) => cache,
            Err(err) => panic!("cache creation error: {}", err),
        };
        cache
    }

    #[tokio::test]
    async fn test_cache_init() {
        init_cache().await;
    }

    #[tokio::test]
    async fn test_cache_account() {
        let cache = init_cache().await;
        let account = Account {
            owner: CachedPubkey::new([0; 32]),
            data: Bytes::from(vec![0; 32]),
            executable: false,
            lamports: 32,
            rent_epoch: 3234,
        };

        let key = AccountKey {
            pubkey: CachedPubkey::new([1; 32]),
            commitment: Commitment::Confirmed,
        };
        let cachable = CacheableAccount {
            key: key.clone(),
            account: account.clone(),
        };
        cache.store_account(cachable).await;
        let acc = cache.get_account(&key).await;
        assert_eq!(acc, Some(Arc::new(account)));
        tokio::time::sleep(Duration::from_secs(3)).await;
        let acc = cache.get_account(&key).await;
        assert_eq!(acc, None);
    }

    #[tokio::test]
    async fn test_cache_program() {
        let cache = init_cache().await;
        let mut accounts = Vec::with_capacity(100);
        for i in 0..100 {
            let account = Account {
                owner: CachedPubkey::new([i; 32]),
                data: Bytes::from(vec![1; (i * 2) as usize]),
                executable: false,
                lamports: i as u64,
                rent_epoch: i as u64,
            };
            let cachable = CacheableAccount {
                key: AccountKey {
                    pubkey: CachedPubkey::new([i + 1; 32]),
                    commitment: Commitment::Processed,
                },
                account,
            };
            accounts.push(cachable);
        }
        let key = ProgramKey {
            pubkey: CachedPubkey::new([0; 32]),
            commitment: Commitment::Processed,
            filters: None,
        };
        cache.store_program(key.clone(), accounts).await;
        let accounts = cache.get_program_accounts(&key).await;
        assert!(accounts.is_some());
        println!("ACC: {:?}", accounts);
        assert_eq!(accounts.unwrap().len(), 100);
        tokio::time::sleep(Duration::from_secs(3)).await;
        let accounts = cache.get_program_accounts(&key).await;
        assert_eq!(accounts, None);
    }
}
