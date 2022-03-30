use std::{borrow::Borrow, collections::HashSet, hash::Hash, sync::Arc};

use serde::{ser::SerializeSeq, Deserialize, Serialize};
use smallvec::SmallVec;

pub struct Account {
    pub owner: Pubkey,
    pub data: Vec<u8>,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub executable: bool,
}

#[cfg_attr(test, derive(Debug))]
pub struct AccountWithKey {
    pub pubkey: Pubkey,
    pub account: Arc<Account>,
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize, Copy)]
pub enum Commitment {
    Processed,
    Confirmed,
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

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Filters(SmallVec<[ProgramFilter; 3]>);

#[derive(Clone, Eq, Hash, PartialEq, Serialize)]
pub enum ProgramFilter {
    DataSize(u64),
    Memcmp(MemcmpFilter),
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize)]
pub struct MemcmpFilter {
    offset: u64,
    bytes: Pattern,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Pattern(SmallVec<[u8; 64]>);

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
#[cfg_attr(test, derive(Debug))]
pub struct Pubkey(pub [u8; 32]);

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct AccountKey {
    pub pubkey: Pubkey,
    pub commitment: Commitment,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ProgramKey {
    pub pubkey: Pubkey,
    pub commitment: Commitment,
    pub filters: Option<Filters>,
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

impl<A> Clone for AccountWithKey {
    fn clone(&self) -> Self {
        let pubkey = self.pubkey;
        let account = Arc::clone(&self.account);
        Self { pubkey, account }
    }
}

impl Borrow<Pubkey> for AccountWithKey {
    fn borrow(&self) -> &Pubkey {
        &self.pubkey
    }
}

impl Pubkey {
    pub fn new(buf: [u8; 32]) -> Self {
        Self(buf)
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

const MAX_BASE58_PUBKEY: usize = 44;
impl Serialize for Pubkey {
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

impl<'de> Deserialize<'de> for Pubkey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Pubkey;

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
                Ok(Pubkey(buf))
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
