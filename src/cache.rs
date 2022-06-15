use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use moka::sync::Cache as MokaCache;
use tokio_tungstenite::tungstenite::Error;

use crate::types::{
    Account, AccountKey, AccountWithKey, CacheHitStatus, Commitment, ProgramAccounts, ProgramKey,
    TransactionKey, TransactionState,
};
use crate::ws::manager::WsConnectionManager;
use crate::ws::notification::ProgramNotification;
use crate::ws::subscription::SubscriptionInfo;
use crate::{metrics::METRICS, types::OptionalAccount};

const CACHE_DISABLED_ENV: &str = "CACHEIUM_DISABLED";
const MAX_SLOT_UPDATE_INTERVAL: Duration = Duration::from_secs(3);

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
    accounts: MokaCache<AccountKey, Arc<OptionalAccount>>,
    programs: MokaCache<ProgramKey, Arc<ProgramAccounts>>,
    transactions: MokaCache<TransactionKey, Arc<TransactionState>>,
    slots: [Arc<AtomicU64>; 3],
    last_slot_update: Arc<RwLock<Instant>>,
}

/// Main type for using library, keeps account and program entries, along with
/// managing their updates via websocket updates, and tracks TTL of each entry
pub struct Cache {
    inner: InnerCache,
    ws: WsConnectionManager,
    /// flag to completely disable cache storage, should be used only for debug purposes,
    /// flag can be controlled via environment variable
    disabled: bool,
}

impl Clone for InnerCache {
    fn clone(&self) -> Self {
        let accounts = self.accounts.clone();
        let programs = self.programs.clone();
        let transactions = self.transactions.clone();
        let slots = self.slots.clone();
        let last_slot_update = Arc::clone(&self.last_slot_update);
        Self {
            accounts,
            programs,
            transactions,
            slots,
            last_slot_update,
        }
    }
}

impl Clone for Cache {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        let ws = self.ws.clone();
        let disabled = self.disabled;
        Self {
            inner,
            ws,
            disabled,
        }
    }
}

impl InnerCache {
    fn new(accounts: u64, programs: u64, transactions: u64) -> Self {
        let accounts = MokaCache::new(accounts);
        let programs = MokaCache::new(programs);
        let transactions = MokaCache::new(transactions);
        let slots = [Arc::default(), Arc::default(), Arc::default()];
        let last_slot_update = Arc::new(RwLock::new(Instant::now()));
        Self {
            accounts,
            programs,
            transactions,
            slots,
            last_slot_update,
        }
    }
    #[inline]
    pub(crate) fn update_slot(&mut self, slot: u64, commitment: Commitment) {
        *self.last_slot_update.write().unwrap() = Instant::now();
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
    pub(crate) fn contains_transaction(&self, key: &TransactionKey) -> bool {
        self.transactions.contains_key(key)
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
    pub(crate) fn remove_transaction(&self, key: &TransactionKey) {
        self.transactions.invalidate(key);
    }

    #[inline]
    pub(crate) fn update_account(&self, key: AccountKey, account: Account) -> bool {
        if !self.accounts.contains_key(&key) {
            return false;
        }
        self.accounts.insert(key, Arc::new(Some(account).into()));
        true
    }

    #[inline]
    pub(crate) fn update_trasaction(&self, key: &TransactionKey, trx: TransactionState) -> bool {
        if !self.transactions.contains_key(key) {
            return false;
        }
        self.transactions.insert(key.clone(), Arc::new(trx));
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
        self.accounts.insert(key, Arc::new(account.into()));
        true
    }
}

impl Cache {
    /// Construct new cache instance. One instance should be cloned and shared
    /// between multiple threads, instead of creating more new instances
    pub async fn new(
        accounts_capacity: u64,
        programs_capacity: u64,
        transactions_capacity: u64,
        ws_url: String,
        connection_count: usize,
    ) -> Result<Self, Error> {
        let inner = InnerCache::new(accounts_capacity, programs_capacity, transactions_capacity);
        let ws = WsConnectionManager::new(ws_url, connection_count, inner.clone()).await?;
        let disabled = std::env::var(CACHE_DISABLED_ENV).is_ok();
        if disabled {
            tracing::warn!("cache is disabled, all cache requests will result in miss");
        }
        let cache = Self {
            inner,
            ws,
            disabled,
        };
        Ok(cache)
    }

    /// Store account information in cache, and subscribe to updates of the given account via
    /// websocket subscriptions, might cause eviction of older values if cache is full
    pub async fn store_account<O: Into<OptionalAccount>>(&self, key: AccountKey, account: O) {
        if self.disabled {
            return;
        }
        if self.inner.accounts.contains_key(&key) {
            return;
        }
        self.inner
            .accounts
            .insert(key.clone(), Arc::new(account.into()));
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
        if self.disabled {
            return;
        }
        if self.inner.programs.contains_key(&key) {
            return;
        }
        let mut program_accounts = Vec::new();
        for a in accounts {
            let record: CacheableAccount = a.into();
            program_accounts.push(record.key.pubkey);
            self.inner
                .accounts
                .insert(record.key, Arc::new(Some(record.account).into()));
        }
        let program_accounts = Arc::new(ProgramAccounts::new(program_accounts));
        self.inner.programs.insert(key.clone(), program_accounts);
        let info = SubscriptionInfo::Program(Box::new(key));
        self.ws.subscribe(info).await;
    }

    /// Start tracking transaction, this will subscribe for transaction signature via websocket,
    /// if transaction gets confirmed or errors, its corresponding state will get updated in cache
    pub async fn track_transaction(&self, key: TransactionKey) {
        if self.disabled {
            return;
        }
        if self.inner.transactions.contains_key(&key) {
            return;
        }
        self.inner
            .transactions
            .insert(key.clone(), Arc::new(TransactionState::Pending));
        let info = SubscriptionInfo::Transaction(key);
        self.ws.subscribe(info).await;
    }

    /// Retrieve account information from cache for the given key,
    /// will reset TTL for the given account entry in cache
    pub fn get_account(&self, key: &AccountKey) -> CacheHitStatus<Arc<Account>> {
        if self.disabled {
            return CacheHitStatus::Miss;
        }
        let value = match self.inner.accounts.get(key) {
            Some(v) => v,
            None => return CacheHitStatus::Miss,
        };
        match value.as_ref() {
            OptionalAccount::Exists(acc) => CacheHitStatus::Hit(Arc::clone(acc)),
            OptionalAccount::Absent => CacheHitStatus::HitButEmpty,
        }
    }

    /// Retrieve program accounts from cache for the given program key,
    /// will reset TTL for the given program entry in cache
    pub fn get_program_accounts(&self, key: &ProgramKey) -> Option<Vec<AccountWithKey>> {
        if self.disabled {
            return None;
        }
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
            let account = match account.as_ref() {
                OptionalAccount::Exists(acc) => Arc::clone(acc),
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

    /// Search for given transaction by its signature and retrieve its state
    pub async fn get_transaction(&self, key: &TransactionKey) -> Option<Arc<TransactionState>> {
        if self.disabled {
            return None;
        }
        self.inner.transactions.get(key)
    }

    /// Load latest slot number for given commitment level
    pub fn get_slot<C: Into<Commitment>>(&self, commitment: C) -> u64 {
        self.inner.slots[commitment.into() as usize].load(Ordering::Relaxed)
    }

    /// perform healthcheck based on recent slot update receival
    pub fn healthcheck(&self) -> Result<u64, &'static str> {
        if self.inner.last_slot_update.read().unwrap().elapsed() > MAX_SLOT_UPDATE_INTERVAL {
            return Err(
                "cacheium didn't receive slot updates recently, unhealthy websocket server",
            );
        }
        let slot = self.inner.slots[Commitment::Processed as usize].load(Ordering::Relaxed);
        Ok(slot)
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
