use std::{collections::HashSet, hash::Hash, sync::Arc};

use prometheus::core::{AtomicI64, AtomicU64, GenericCounter, GenericGauge};
use serde::{ser::SerializeSeq, Deserialize, Serialize};
use smallvec::SmallVec;

use crate::{
    metrics::{ACCOUNTS, METRICS, PROGRAMS},
    ws::subscription::SubMeta,
};

/// Helper trait to indicate how much memory is occupied by cache entry
pub trait Record {
    /// Gets metrics gauge to the size of corresponding cache
    fn size_metrics() -> GenericGauge<AtomicI64>;
    /// Gets metrics gauge to the entries count in corresponding cache
    fn count_metrics() -> GenericGauge<AtomicI64>;
    /// Get the count to the number of evictions from given cache
    fn eviction_metrics() -> GenericCounter<AtomicU64>;
    /// Gets the number of bytes this type instance occupies in memory
    fn size(&self) -> usize;
}

/// Container for account data,
/// it is the one stored in cache
#[cfg_attr(test, derive(Debug, PartialEq, Eq, Clone))]
pub struct Account {
    /// Public key of account's owner program
    pub owner: CachedPubkey,
    /// Binary data of given account
    pub data: Vec<u8>,
    /// Account's balance
    pub lamports: u64,
    /// Next epoch, when account will owe the rent fee
    pub rent_epoch: u64,
    /// Weather account can be executed as a program
    pub executable: bool,
}

/// Container for holding account's pubkey along with its state
pub struct AccountWithKey {
    /// Pubkey of account
    pub pubkey: CachedPubkey,
    /// Shared reference to account's state
    pub account: Arc<Account>,
}

/// Commitment levels
#[derive(Clone, Eq, Hash, PartialEq, Serialize, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    /// Processed commitment level
    Processed,
    /// Confirmed commitment level
    Confirmed,
    /// Finalized commitment level
    Finalized,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Encoding {
    Base58,
    Base64,
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
}

pub(crate) struct CacheValue<T> {
    pub value: T,
    pub sub: Option<SubMeta>,
}

/// Describes three possible outcomes of cache query
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub enum CacheHitStatus<T> {
    /// Value is present in cache
    Hit(T),
    /// Value is absent in cache, but has subscription for it setup
    /// this allowes to avoid queries to upstream database for non-existent entries
    HitButEmpty,
    /// Value is not present in cache
    Miss,
}

#[derive(Clone)]
pub(crate) struct ProgramAccounts(HashSet<CachedPubkey>);

/// Wrapper type for program filters array
#[derive(Clone, Eq, Hash, PartialEq, Default)]
pub struct Filters(SmallVec<[ProgramFilter; 3]>);

/// Program filters, see https://docs.solana.com/developing/clients/jsonrpc-api#filters
#[derive(Clone, Eq, Hash, PartialEq, Serialize, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum ProgramFilter {
    /// Filters by length of account's data field
    DataSize(u64),
    /// Compares slice of account's data field with provided pattern
    Memcmp(MemcmpFilter),
}

/// Offset and pattern to perform memory comparison
#[derive(Clone, Eq, Hash, PartialEq, Serialize, PartialOrd, Ord)]
pub struct MemcmpFilter {
    /// offset to start matching byte patter
    pub offset: usize,
    /// byte pattern to match against
    pub bytes: Pattern,
}

/// Sequence of bytes to memory compare with
#[derive(Clone, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct Pattern(SmallVec<[u8; 32]>);

/// Public key of account/program
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
#[cfg_attr(test, derive(Debug))]
pub struct CachedPubkey([u8; 32]);

/// Cache key, uniquely identifying account record
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct AccountKey {
    /// Public key of account
    pub pubkey: CachedPubkey,
    /// Commitment level, the account was stored at
    pub commitment: Commitment,
}

/// Cache key, uniquely identifying program record
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ProgramKey {
    /// Public key of program
    pub pubkey: CachedPubkey,
    /// Commitment level, the program was stored at
    pub commitment: Commitment,
    /// Optional program filters, for narrowing accounts list down
    pub filters: Option<Filters>,
}

impl Record for Account {
    fn size_metrics() -> GenericGauge<AtomicI64> {
        METRICS.cache_size.with_label_values(ACCOUNTS)
    }
    fn count_metrics() -> GenericGauge<AtomicI64> {
        METRICS.cached_entries.with_label_values(ACCOUNTS)
    }
    fn eviction_metrics() -> GenericCounter<AtomicU64> {
        METRICS.evictions.with_label_values(ACCOUNTS)
    }
    fn size(&self) -> usize {
        let constant = std::mem::size_of::<Self>();
        let dynamic = self.data.capacity();
        constant + dynamic
    }
}

impl Record for Option<Arc<Account>> {
    fn size_metrics() -> GenericGauge<AtomicI64> {
        METRICS.cache_size.with_label_values(ACCOUNTS)
    }
    fn eviction_metrics() -> GenericCounter<AtomicU64> {
        METRICS.evictions.with_label_values(ACCOUNTS)
    }
    fn count_metrics() -> GenericGauge<AtomicI64> {
        METRICS.cached_entries.with_label_values(ACCOUNTS)
    }
    fn size(&self) -> usize {
        match self {
            Some(acc) => acc.size(),
            None => std::mem::size_of::<Self>(),
        }
    }
}

impl Record for ProgramAccounts {
    fn size_metrics() -> GenericGauge<AtomicI64> {
        METRICS.cache_size.with_label_values(PROGRAMS)
    }
    fn eviction_metrics() -> GenericCounter<AtomicU64> {
        METRICS.evictions.with_label_values(PROGRAMS)
    }
    fn count_metrics() -> GenericGauge<AtomicI64> {
        METRICS.cached_entries.with_label_values(PROGRAMS)
    }
    fn size(&self) -> usize {
        let keysize = std::mem::size_of::<CachedPubkey>();
        self.0.capacity() * keysize
    }
}

impl<V: Record> Record for CacheValue<V> {
    fn size(&self) -> usize {
        let meta = std::mem::size_of::<Option<SubMeta>>();
        self.value.size() + meta
    }
    fn eviction_metrics() -> GenericCounter<AtomicU64> {
        V::eviction_metrics()
    }
    fn count_metrics() -> GenericGauge<AtomicI64> {
        V::count_metrics()
    }
    fn size_metrics() -> GenericGauge<AtomicI64> {
        V::size_metrics()
    }
}

impl From<&[u8]> for Pattern {
    fn from(slice: &[u8]) -> Self {
        let inner = SmallVec::from_slice(slice);
        Self(inner)
    }
}

impl AsRef<[u8]> for Pattern {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl CachedPubkey {
    /// Construct new Public key from array
    pub fn new(buf: [u8; 32]) -> Self {
        Self(buf)
    }

    /// Copy inner array
    pub fn bytes(&self) -> [u8; 32] {
        self.0
    }
}

impl AsRef<[u8]> for CachedPubkey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl MemcmpFilter {
    /// Construct new memore compare filter
    pub fn new(offset: usize, bytes: Pattern) -> Self {
        Self { offset, bytes }
    }
}

impl From<u8> for Commitment {
    fn from(num: u8) -> Self {
        match num {
            0 => Self::Processed,
            1 => Self::Confirmed,
            _ => Self::Finalized,
        }
    }
}

impl ProgramAccounts {
    pub fn new<I: IntoIterator<Item = CachedPubkey>>(accounts: I) -> Self {
        Self(accounts.into_iter().collect())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn insert(&mut self, key: CachedPubkey) {
        self.0.insert(key);
    }
}

impl IntoIterator for ProgramAccounts {
    type Item = CachedPubkey;
    type IntoIter = <HashSet<CachedPubkey> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

const MAX_BASE58_PUBKEY: usize = 44; // taken from validator
impl Serialize for CachedPubkey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = [0_u8; MAX_BASE58_PUBKEY];
        let len = bs58::encode(self.0.as_ref()).into(buf.as_mut()).unwrap();
        let key = std::str::from_utf8(&buf[..len]).unwrap();
        serializer.serialize_str(key)
    }
}

impl<'de> Deserialize<'de> for CachedPubkey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = CachedPubkey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("base58 encoded string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let mut buf = [0; 32];
                bs58::decode(v)
                    .into(buf.as_mut())
                    .map_err(|e| serde::de::Error::custom(e))?;
                Ok(CachedPubkey(buf))
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

const MAX_BASE58_PATTERN: usize = 175;
impl Serialize for Pattern {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = [0_u8; MAX_BASE58_PATTERN];
        let len = bs58::encode(self.0.as_ref()).into(buf.as_mut()).unwrap();
        let key = std::str::from_utf8(&buf[..len]).unwrap();
        serializer.serialize_str(key)
    }
}

impl Serialize for Filters {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for f in self.0.iter() {
            seq.serialize_element(f)?;
        }
        seq.end()
    }
}

impl Filters {
    /// Add new program filter to existing list
    pub fn insert(&mut self, f: ProgramFilter) {
        self.0.push(f);
        self.0.sort_unstable(); // sort, as Eq and Hash depend on sort order
    }
}

impl Commitment {
    /// Check whether commitment is finalized
    pub fn finalized(&self) -> bool {
        matches!(self, Self::Finalized)
    }
}

impl FromIterator<ProgramFilter> for Filters {
    #[inline]
    fn from_iter<T: IntoIterator<Item = ProgramFilter>>(iter: T) -> Self {
        let mut inner: SmallVec<[ProgramFilter; 3]> = iter.into_iter().collect();
        inner.sort_unstable();

        Self(inner)
    }
}
