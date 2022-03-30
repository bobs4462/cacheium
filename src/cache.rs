use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use dashmap::DashMap;
use futures_delay_queue::{delay_queue, DelayHandle, DelayQueue as FutureDelayQueue, Receiver};
use futures_intrusive::buffer::GrowingHeapBuf;
use tokio::select;
use tokio_tungstenite::tungstenite::Error;

use crate::{
    types::{Account, AccountKey, AccountWithKey, ProgramAccounts, ProgramKey},
    ws::manager::WsConnectionManager,
    ws::notification::ProgramNotification,
    ws::subscription::{SubMeta, SubscriptionInfo},
};

type DelayQueue<T> = Arc<Mutex<FutureDelayQueue<T, GrowingHeapBuf<T>>>>;

pub struct CacheableAccount {
    pub key: AccountKey,
    pub account: Account,
    pub slot: u64,
}

pub struct InnerCache {
    accounts: Arc<DashMap<AccountKey, CacheValue<Arc<Account>>>>,
    programs: Arc<DashMap<ProgramKey, CacheValue<ProgramAccounts>>>,
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
    pub slot: u64,
    pub refs: usize,
}

impl Clone for InnerCache {
    fn clone(&self) -> Self {
        let accounts = Arc::clone(&self.accounts);
        let programs = Arc::clone(&self.programs);
        Self { accounts, programs }
    }
}

impl Default for InnerCache {
    fn default() -> Self {
        let accounts = Arc::default();
        let programs = Arc::default();
        Self { accounts, programs }
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
    fn new(value: T, handle: Option<DelayHandle>, slot: u64) -> Self {
        Self {
            value,
            handle,
            sub: None,
            slot,
            refs: 1,
        }
    }

    pub fn set_subscription(&mut self, sub: SubMeta) {
        self.sub.replace(sub);
    }

    #[inline]
    pub fn set_value(&mut self, value: T, slot: u64) {
        self.value = value;
        self.slot = slot;
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
    pub(crate) fn update_account(&self, key: &AccountKey, account: Account, slot: u64) {
        if let Some(mut v) = self.accounts.get_mut(key) {
            v.set_value(Arc::new(account), slot);
        }
    }

    #[inline]
    pub(crate) fn upsert_program_account(
        &mut self,
        notification: ProgramNotification,
        programkey: &ProgramKey,
        slot: u64,
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
            v.set_value(account, slot);
        } else {
            let value = CacheValue::new(account, None, slot);
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
        let account = record.into();
        let aqueue = self.aqueue.lock().unwrap();
        let delay = aqueue.insert(account.key.clone(), self.ttl);
        let value = CacheValue::new(Arc::new(account), Some(delay), account.slot);
        self.inner.accounts.insert(account.key.clone(), value);
        let info = SubscriptionInfo::Account(account.key);
        self.ws.subscribe(info).await;
    }

    pub async fn store_program<C: Into<CacheableAccount>>(
        &self,
        key: ProgramKey,
        accounts: Vec<C>,
        slot: u64,
    ) {
        let mut to_unsubscribe = Vec::new();
        for a in &accounts {
            let accountkey = AccountKey {
                pubkey: a.pubkey,
                commitment: key.commitment,
            };
            if let Some(mut acc) = self.inner.accounts.get_mut(&accountkey) {
                acc.refs += 1;
                if let Some(handle) = acc.handle.take() {
                    let _ = handle.cancel().await;
                }
                if let Some(meta) = acc.sub.take() {
                    to_unsubscribe.push(meta);
                }
            } else {
                let value = CacheValue::new(Arc::clone(&a.account), None, slot);
                self.inner.accounts.insert(accountkey, value);
            }
        }
        let pqueue = self.pqueue.lock().unwrap();
        let delay = pqueue.insert(key.clone(), self.ttl);
        let value = CacheValue::new(ProgramAccounts::new(accounts), Some(delay), slot);
        self.inner.programs.insert(key.clone(), value);
        let info = SubscriptionInfo::Program(Box::new(key));
        self.ws.subscribe(info).await;
        for m in to_unsubscribe {
            self.ws.unsubscribe(m).await;
        }
    }

    pub async fn get_account(&self, key: &AccountKey) -> Option<(Arc<Account>, u64)> {
        let mut res = self.inner.accounts.get_mut(key)?;
        if res.handle.is_some() {
            res.reset_delay(self.ttl).await;
        } else {
            let aqueue = self.aqueue.lock().unwrap();
            let delay = aqueue.insert(key.clone(), self.ttl);
            res.set_delay(delay);
        }
        Some((Arc::clone(&res.value), res.slot))
    }

    pub async fn get_program_accounts(
        &self,
        key: &ProgramKey,
    ) -> Option<(Vec<AccountWithKey>, u64)> {
        let mut res = self.inner.programs.get_mut(key)?;
        res.reset_delay(self.ttl).await;
        let accounts = res.value().value.accounts().iter().cloned().collect();

        Some((accounts, res.slot))
    }

    async fn remove_account(&self, key: AccountKey) {
        let sub = self.inner.accounts.remove(&key).and_then(|(_, v)| v.sub);

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

    use crate::{
        cache::CacheableAccount,
        types::{Account, AccountKey, AccountWithKey, Commitment, ProgramKey, Pubkey},
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
            owner: Pubkey::new([0; 32]),
            data: vec![0; 32],
            executable: false,
            lamports: 32,
            rent_epoch: 3234,
        };

        let key = AccountKey {
            pubkey: Pubkey::new([1; 32]),
            commitment: Commitment::Confirmed,
        };
        let account = CacheableAccount {
            key: key.clone(),
            account: account.clone(),
            slot: 1,
        };
        cache.store_account(account).await;
        let acc = cache.get_account(&key).await;
        let account = Arc::new(account);
        assert_eq!(acc, Some((account, 1)));
        tokio::time::sleep(Duration::from_secs(3)).await;
        let acc = cache.get_account(&key).await;
        assert_eq!(acc, None);
    }

    #[tokio::test]
    async fn test_cache_program() {
        let cache = init_cache().await;
        let mut accounts = Vec::with_capacity(100);
        for i in 0..100 {
            let account = Arc::new(Account {
                owner: Pubkey::new([i; 32]),
                data: vec![1; (i * 2) as usize],
                executable: false,
                lamports: i as u64,
                rent_epoch: i as u64,
            });
            let pubkey = Pubkey::new([i + 1; 32]);
            let account = AccountWithKey { pubkey, account };
            accounts.push(account);
        }
        let key = ProgramKey {
            pubkey: Pubkey::new([0; 32]),
            commitment: Commitment::Processed,
            filters: None,
        };
        cache.store_program(key.clone(), accounts, 1).await;
        let accounts = cache.get_program_accounts(&key).await;
        assert!(accounts.is_some());
        println!("ACC: {:?}", accounts);
        assert_eq!(accounts.unwrap().0.len(), 100);
        tokio::time::sleep(Duration::from_secs(3)).await;
        let accounts = cache.get_program_accounts(&key).await;
        assert_eq!(accounts, None);
    }
}
