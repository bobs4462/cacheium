use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use dashmap::DashMap;
use futures_delay_queue::{delay_queue, DelayHandle, DelayQueue as FutureDelayQueue, Receiver};
use futures_intrusive::buffer::GrowingHeapBuf;
use tokio::select;

use crate::{
    types::{Account, AccountKey, AccountWithKey, ProgramAccounts, ProgramKey},
    ws::manager::WsConnectionManager,
    ws::notification::ProgramNotification,
    ws::subscription::{SubMeta, SubscriptionInfo},
};

const TTL: Duration = Duration::from_secs(10 * 60);

type DelayQueue<T> = Arc<Mutex<FutureDelayQueue<T, GrowingHeapBuf<T>>>>;

#[derive(Clone, Default)]
pub struct InnerCache {
    pub accounts: Arc<DashMap<AccountKey, CacheValue<Arc<Account>>>>,
    pub programs: Arc<DashMap<ProgramKey, CacheValue<ProgramAccounts>>>,
}

#[derive(Clone)]
pub struct Cache {
    inner: InnerCache,
    arx: Receiver<AccountKey>,
    prx: Receiver<ProgramKey>,
    aqueue: DelayQueue<AccountKey>,
    pqueue: DelayQueue<ProgramKey>,
    ws: WsConnectionManager,
}

pub struct CacheValue<T> {
    pub value: T,
    pub handle: Option<DelayHandle>,
    pub sub: Option<SubMeta>,
    pub refs: usize,
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

    pub fn set_value<P: Into<T>>(&mut self, value: P) {
        self.value = value.into();
    }

    pub fn set_delay(&mut self, delay: DelayHandle) {
        self.handle.replace(delay);
    }

    pub async fn reset_delay(&mut self) {
        if let Some(handle) = self.handle.take() {
            if let Ok(handle) = handle.reset(TTL).await {
                self.handle = Some(handle);
            }
        }
    }
}

impl InnerCache {
    pub fn upsert_program_account(
        &mut self,
        notification: ProgramNotification,
        programkey: &ProgramKey,
    ) {
        let account = notification.account.into();
        if let Some(mut v) = self.programs.get_mut(programkey) {
            if !v.value.0.contains(&notification.pubkey) {
                let acc = AccountWithKey {
                    pubkey: notification.pubkey,
                    account: Arc::clone(&account),
                };
                v.value.0.insert(acc);
            }
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
    pub async fn new(websocket_url: String, connection_count: usize) -> Self {
        let inner = InnerCache::default();
        let (aqueue, arx) = delay_queue();
        let (pqueue, prx) = delay_queue();
        let ws = WsConnectionManager::new(websocket_url, connection_count, inner.clone()).await;
        let cache = Self {
            inner,
            arx,
            prx,
            aqueue: Arc::new(Mutex::new(aqueue)),
            pqueue: Arc::new(Mutex::new(pqueue)),
            ws,
        };
        let clone = cache.clone();
        tokio::spawn(clone.cleanup());
        cache
    }

    pub async fn store_account(&self, key: AccountKey, account: Account) {
        let aqueue = self.aqueue.lock().unwrap();
        let delay = aqueue.insert(key.clone(), TTL);
        let value = CacheValue::new(Arc::new(account), Some(delay));
        self.inner.accounts.insert(key.clone(), value);
        let info = SubscriptionInfo::Account(key);
        self.ws.subscribe(info).await;
    }

    pub async fn store_program(&self, key: ProgramKey, accounts: HashSet<AccountWithKey>) {
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
                let value = CacheValue::new(Arc::clone(&a.account), None);
                self.inner.accounts.insert(accountkey, value);
            }
        }
        let pqueue = self.pqueue.lock().unwrap();
        let delay = pqueue.insert(key.clone(), TTL);
        let value = CacheValue::new(ProgramAccounts(accounts), Some(delay));
        self.inner.programs.insert(key.clone(), value);
        let info = SubscriptionInfo::Program(key);
        self.ws.subscribe(info).await;
        for m in to_unsubscribe {
            self.ws.unsubscribe(m).await;
        }
    }

    pub async fn get_account(&self, key: &AccountKey) -> Option<Arc<Account>> {
        let mut res = self.inner.accounts.get_mut(key)?;
        if res.handle.is_some() {
            res.reset_delay().await;
        } else {
            let aqueue = self.aqueue.lock().unwrap();
            let delay = aqueue.insert(key.clone(), TTL);
            res.set_delay(delay);
        }
        Some(Arc::clone(&res.value))
    }

    pub async fn get_program_accounts(&self, key: &ProgramKey) -> Option<HashSet<AccountWithKey>> {
        let mut res = self.inner.programs.get_mut(key)?;
        res.reset_delay().await;
        let accounts = res.value().value.0.clone();

        Some(accounts)
    }

    async fn remove_account(&self, key: AccountKey) {
        let sub = self.inner.accounts.remove(&key).and_then(|(_, v)| v.sub);

        if let Some(meta) = sub {
            self.ws.unsubscribe(meta).await;
        }
    }

    async fn remove_program(&self, key: ProgramKey) {
        if let Some((_, v)) = self.inner.programs.remove(&key) {
            let keys = v.value.0;
            let commitment = key.commitment;
            for pubkey in keys.into_iter().map(|acc| acc.pubkey) {
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
                Some(sub) => self.ws.unsubscribe(sub).await,
                None => println!("no subscription meta is found for cache entry"),
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
