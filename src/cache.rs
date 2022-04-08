use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use futures_delay_queue::{delay_queue, DelayHandle, DelayQueue as FutureDelayQueue, Receiver};
use futures_intrusive::buffer::GrowingHeapBuf;
use tokio::{select, sync::Mutex};
use tokio_tungstenite::tungstenite::Error;

use crate::{
    types::{Account, AccountKey, AccountWithKey, Commitment, ProgramAccounts, ProgramKey},
    ws::manager::WsConnectionManager,
    ws::notification::ProgramNotification,
    ws::subscription::{SubMeta, SubscriptionInfo},
};

type DelayQueue<T> = Arc<Mutex<FutureDelayQueue<T, GrowingHeapBuf<T>>>>;

pub struct CacheableAccount {
    pub key: AccountKey,
    pub account: Account,
}

pub struct InnerCache {
    accounts: Arc<DashMap<AccountKey, CacheValue<Arc<Account>>>>,
    programs: Arc<DashMap<ProgramKey, CacheValue<ProgramAccounts>>>,
    slots: [Arc<AtomicU64>; 3],
}

pub struct Cache {
    inner: InnerCache,
    arx: Receiver<AccountKey>,
    prx: Receiver<ProgramKey>,
    aqueue: DelayQueue<AccountKey>,
    pqueue: DelayQueue<ProgramKey>,
    ws: WsConnectionManager,
    ttl: Duration,
}

pub struct CacheValue<T> {
    pub value: T,
    pub handle: Option<DelayHandle>,
    pub sub: Option<SubMeta>,
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
            refs: 1,
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

    #[inline]
    pub async fn reset_delay(&mut self, ttl: Duration) {
        if let Some(handle) = self.handle.take() {
            if let Ok(handle) = handle.reset(ttl).await {
                self.handle = Some(handle);
            }
        }
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
        tokio::spawn(clone.cleanup());
        Ok(cache)
    }

    pub async fn store_account<C: Into<CacheableAccount>>(&self, record: C) {
        let record = record.into();
        let aqueue = self.aqueue.lock().await;
        let delay = aqueue.insert(record.key.clone(), self.ttl);
        let value = CacheValue::new(Arc::new(record.account), Some(delay));
        self.inner.accounts.insert(record.key.clone(), value);
        let info = SubscriptionInfo::Account(record.key);
        self.ws.subscribe(info).await;
    }

    pub async fn store_program<C: Into<CacheableAccount>>(
        &self,
        key: ProgramKey,
        accounts: Vec<C>,
    ) {
        let mut to_unsubscribe = Vec::new();
        let mut program_accounts = Vec::new();
        for a in accounts {
            let record: CacheableAccount = a.into();
            if let Some(mut entry) = self.inner.accounts.get_mut(&record.key) {
                entry.refs += 1;
                if let Some(handle) = entry.handle.take() {
                    let _ = handle.cancel().await;
                }
                if let Some(meta) = entry.sub.take() {
                    to_unsubscribe.push(meta);
                }
                let account_with_key = AccountWithKey {
                    pubkey: record.key.pubkey,
                    account: Arc::clone(&entry.value),
                };
                program_accounts.push(account_with_key);
            } else {
                let account = Arc::new(record.account);
                let value = CacheValue::new(Arc::clone(&account), None);
                let account_with_key = AccountWithKey {
                    pubkey: record.key.pubkey,
                    account,
                };
                self.inner.accounts.insert(record.key, value);
                program_accounts.push(account_with_key);
            }
        }
        let pqueue = self.pqueue.lock().await;
        let delay = pqueue.insert(key.clone(), self.ttl);
        let value = CacheValue::new(ProgramAccounts::new(program_accounts), Some(delay));
        self.inner.programs.insert(key.clone(), value);
        let info = SubscriptionInfo::Program(Box::new(key));
        self.ws.subscribe(info).await;
        for m in to_unsubscribe {
            self.ws.unsubscribe(m).await;
        }
    }

    pub async fn get_account(&self, key: &AccountKey) -> Option<Arc<Account>> {
        let mut res = self.inner.accounts.get_mut(key)?;
        if res.handle.is_some() {
            res.reset_delay(self.ttl).await;
        } else {
            let aqueue = self.aqueue.lock().await;
            let delay = aqueue.insert(key.clone(), self.ttl);
            res.set_delay(delay);
            res.refs += 1;
        }
        Some(Arc::clone(&res.value))
    }

    pub async fn get_program_accounts(&self, key: &ProgramKey) -> Option<Vec<AccountWithKey>> {
        let mut res = self.inner.programs.get_mut(key)?;
        res.reset_delay(self.ttl).await;
        let accounts = res.value.accounts().iter().cloned().collect();

        Some(accounts)
    }

    pub fn get_slot<C: Into<Commitment>>(&self, commitment: C) -> u64 {
        self.inner.slots[commitment.into() as usize].load(Ordering::Relaxed)
    }

    async fn remove_account(&self, key: AccountKey) {
        let sub = self
            .inner
            .accounts
            .remove_if(&key, |_, v| v.refs == 1)
            .and_then(|(_, v)| v.sub);

        if let Some(meta) = sub {
            self.ws.unsubscribe(meta).await;
        } else if let Some(mut acc) = self.inner.accounts.get_mut(&key) {
            acc.refs -= 1;
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
                let removed = self
                    .inner
                    .accounts
                    .remove_if(&key, |_, v| v.handle.is_none() && v.refs < 2)
                    .is_some();
                if !removed {
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
