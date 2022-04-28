use std::{borrow::Borrow, collections::HashSet, hash::Hash, sync::Arc};

use bytes::Bytes;
use serde::{ser::SerializeSeq, Deserialize, Serialize};
use smallvec::SmallVec;

/// Container for account data,
/// it is the one stored in cache
#[cfg_attr(test, derive(Debug, PartialEq, Eq, Clone))]
pub struct Account {
    /// Public key of account's owner program
    pub owner: CachedPubkey,
    /// Binary data of given account
    pub data: Bytes,
    /// Account's balance
    pub lamports: u64,
    /// Next epoch, when account will owe the rent fee
    pub rent_epoch: u64,
    /// Weather account can be executed as a program
    pub executable: bool,
}

/// Wrapper type for storing account information along with its public key
#[cfg_attr(test, derive(Debug))]
pub struct AccountWithKey {
    /// Public key of account
    pub pubkey: CachedPubkey,
    /// Shared reference to account's information
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

pub(crate) struct ProgramAccounts(HashSet<AccountWithKey>);

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
    offset: usize,
    bytes: Pattern,
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

impl From<&[u8]> for Pattern {
    fn from(slice: &[u8]) -> Self {
        let inner = SmallVec::from_slice(slice);
        Self(inner)
    }
}

impl Hash for AccountWithKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pubkey.hash(state)
    }
}
impl PartialEq for AccountWithKey {
    fn eq(&self, other: &Self) -> bool {
        self.pubkey == other.pubkey
    }
}
impl Eq for AccountWithKey {}

impl Clone for AccountWithKey {
    fn clone(&self) -> Self {
        let pubkey = self.pubkey;
        let account = Arc::clone(&self.account);
        Self { pubkey, account }
    }
}

impl Borrow<CachedPubkey> for AccountWithKey {
    fn borrow(&self) -> &CachedPubkey {
        &self.pubkey
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
    pub fn new<I: IntoIterator<Item = AccountWithKey>>(accounts: I) -> Self {
        Self(accounts.into_iter().collect())
    }

    #[inline]
    pub(crate) fn accounts(&self) -> &HashSet<AccountWithKey> {
        &self.0
    }
    #[inline]
    pub(crate) fn accounts_mut(&mut self) -> &mut HashSet<AccountWithKey> {
        &mut self.0
    }

    pub(crate) fn into_iter(self) -> impl Iterator<Item = AccountWithKey> {
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
