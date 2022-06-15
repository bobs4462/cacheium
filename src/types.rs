use std::{collections::HashSet, fmt::Display, hash::Hash, sync::Arc};

use serde::{ser::SerializeSeq, Deserialize, Serialize};
use smallvec::SmallVec;

use crate::metrics::{ACCOUNTS, METRICS, PROGRAMS, TRANSACTIONS};

/// Container for account data,
/// it is the one stored in cache
#[cfg_attr(test, derive(Debug, PartialEq, Eq, Clone))]
pub struct Account {
    /// Public key of account's owner program
    pub(crate) owner: CachedPubkey,
    /// Binary data of given account
    pub(crate) data: Vec<u8>,
    /// Account's balance
    pub(crate) lamports: u64,
    /// Next epoch, when account will owe the rent fee
    pub(crate) rent_epoch: u64,
    /// Weather account can be executed as a program
    pub(crate) executable: bool,
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

/// Signature of transaction
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
#[cfg_attr(test, derive(Debug))]
pub struct CachedSignature([u8; 64]);

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

/// Cache key, uniquely identifying single transaction
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct TransactionKey {
    /// Transaction signature
    pub signature: CachedSignature,
    /// Commitment level of transaction confirmation,
    /// only `Finalized` and `Confirmed` levels are supported
    pub commitment: Commitment,
}

/// Option like enum for holding either
/// account itself or a placeholder in cache
pub enum OptionalAccount {
    /// Account is present
    Exists(Arc<Account>),
    /// Placeholder for account
    Absent,
}

/// Current state of signatureSubscribe
/// _Note_: none of the enum variants should be used to construct \
/// new instance or otherwise it will mess up metrics computation,\
/// use `Default::default` instead
pub enum TransactionState {
    /// Transaction is ready to be fetched from database
    Ready,
    /// Transaction hasn't been confirmed by solana blockchain yet
    Pending,
    /// Error occured during transaction confirmation
    Error(json::Value),
}

impl OptionalAccount {
    /// Initialize new instance of account, along with updating cache metrics
    pub fn exists(acc: Account) -> Self {
        METRICS.cached_entries.with_label_values(ACCOUNTS).inc();
        let constant = std::mem::size_of::<Self>();
        let dynamic = acc.data.capacity();
        METRICS
            .cache_size
            .with_label_values(ACCOUNTS)
            .add((constant + dynamic) as i64);

        Self::Exists(Arc::new(acc))
    }

    /// Create cache placeholder for nonexistent account
    pub fn empty() -> Self {
        METRICS.cached_entries.with_label_values(ACCOUNTS).inc();
        let constant = std::mem::size_of::<Self>();
        METRICS
            .cache_size
            .with_label_values(ACCOUNTS)
            .add(constant as i64);
        Self::Absent
    }
}

impl Default for TransactionState {
    fn default() -> Self {
        METRICS.cached_entries.with_label_values(TRANSACTIONS).inc();
        let constant = std::mem::size_of::<Self>();
        METRICS
            .cache_size
            .with_label_values(TRANSACTIONS)
            .add(constant as i64);
        Self::Pending
    }
}

impl Account {
    /// Create new instance of Account
    pub fn new(
        owner: CachedPubkey,
        data: Vec<u8>,
        lamports: u64,
        rent_epoch: u64,
        executable: bool,
    ) -> Self {
        Self {
            owner,
            data,
            lamports,
            rent_epoch,
            executable,
        }
    }
    /// Get number account's lamports
    pub fn lamports(&self) -> u64 {
        self.lamports
    }

    /// Get a reference to account's data field
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get copy of pubkey of account's owner
    pub fn owner(&self) -> CachedPubkey {
        self.owner
    }

    /// Get epoch when account will owne a rent
    pub fn rent_epoch(&self) -> u64 {
        self.rent_epoch
    }

    /// Get flag whether account is executable
    pub fn executable(&self) -> bool {
        self.executable
    }
}

impl From<Option<Account>> for OptionalAccount {
    fn from(account: Option<Account>) -> Self {
        match account {
            Some(acc) => Self::exists(acc),
            None => Self::empty(),
        }
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

impl CachedSignature {
    /// Construct new Public key from array
    pub fn new(buf: [u8; 64]) -> Self {
        Self(buf)
    }

    /// Copy inner array
    pub fn bytes(&self) -> [u8; 64] {
        self.0
    }
}

impl AsRef<[u8]> for CachedPubkey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for CachedSignature {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl MemcmpFilter {
    /// Construct new memory compare filter
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
    pub(crate) fn new<I: IntoIterator<Item = CachedPubkey>>(accounts: I) -> Self {
        METRICS.cached_entries.with_label_values(PROGRAMS).inc();
        let keys = Self(accounts.into_iter().collect());
        let keysize = std::mem::size_of::<CachedPubkey>();
        let size = keys.0.capacity() * keysize;
        METRICS
            .cache_size
            .with_label_values(PROGRAMS)
            .add(size as i64);
        keys
    }

    pub(crate) fn clone_with_key(self: Arc<Self>, key: CachedPubkey) -> Self {
        let mut clone = self.0.clone();
        clone.insert(key);
        Self::new(clone)
    }

    pub(crate) fn contains(&self, key: &CachedPubkey) -> bool {
        self.0.contains(key)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &CachedPubkey> {
        self.0.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
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

const MAX_BASE58_SIGNATURE: usize = 88; // taken from validator
impl Serialize for CachedSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = [0_u8; MAX_BASE58_SIGNATURE];
        let len = bs58::encode(self.0.as_ref()).into(buf.as_mut()).unwrap();
        let key = std::str::from_utf8(&buf[..len]).unwrap();
        serializer.serialize_str(key)
    }
}

impl Display for CachedPubkey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key = bs58::encode(&self.0).into_string();
        write!(f, "{}", key)
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

impl Drop for OptionalAccount {
    fn drop(&mut self) {
        METRICS.cached_entries.with_label_values(ACCOUNTS).dec();
        let constant = std::mem::size_of::<Self>();
        let dynamic = match self {
            Self::Exists(acc) => acc.data.capacity(),
            Self::Absent => 0,
        };
        METRICS
            .cache_size
            .with_label_values(ACCOUNTS)
            .sub((constant + dynamic) as i64);
    }
}

impl Drop for ProgramAccounts {
    fn drop(&mut self) {
        METRICS.cached_entries.with_label_values(PROGRAMS).dec();
        let keysize = std::mem::size_of::<CachedPubkey>();
        let size = self.0.capacity() * keysize;
        METRICS
            .cache_size
            .with_label_values(PROGRAMS)
            .sub(size as i64);
    }
}

impl Drop for TransactionState {
    fn drop(&mut self) {
        METRICS.cached_entries.with_label_values(TRANSACTIONS).dec();
        let size = std::mem::size_of::<Self>();
        METRICS
            .cache_size
            .with_label_values(PROGRAMS)
            .sub(size as i64);
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
