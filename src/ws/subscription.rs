use std::{borrow::Borrow, fmt::Display};

use serde::ser::SerializeSeq;
use serde::Serialize;

use crate::types::{
    AccountKey, CachedPubkey, CachedSignature, Commitment, Encoding, Filters, ProgramKey,
    TransactionKey,
};

const JSONRPC: &str = "2.0";
const ACCOUNT_SUBSCRIBE: &str = "accountSubscribe";
pub const ACCOUNT_UNSUBSCRIBE: &str = "accountUnsubscribe";
const PROGRAM_SUBSCRIBE: &str = "programSubscribe";
pub const PROGRAM_UNSUBSCRIBE: &str = "programUnsubscribe";
const SIGNATURE_SUBSCRIBE: &str = "signatureSubscribe";
pub const SIGNATURE_UNSUBSCRIBE: &str = "signatureUnsubscribe";
const SLOT_SUBSCRIBE: &str = "slotSubscribe";

#[derive(Serialize)]
#[serde(untagged)]
enum EitherKey<'a> {
    Pubkey(&'a CachedPubkey),
    Signature(&'a CachedSignature),
}

#[derive(Clone)]
pub enum SubscriptionInfo {
    Account(AccountKey),
    Program(Box<ProgramKey>),
    Transaction(TransactionKey),
    Slot,
}

#[derive(Serialize)]
pub struct SubRequest<'a> {
    jsonrpc: &'a str,
    pub id: u64,
    method: &'a str,
    params: Option<SubParams<'a>>,
}

#[derive(Serialize)]
pub struct UnsubRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: [u64; 1],
}

pub struct SubParams<'a> {
    key: EitherKey<'a>,
    config: SubConfig<'a>,
}

#[derive(Serialize)]
pub struct SubConfig<'a> {
    commitment: Commitment,
    encoding: Option<Encoding>,
    filters: Option<&'a Filters>,
}

impl<'a> From<&'a SubscriptionInfo> for SubRequest<'a> {
    fn from(info: &'a SubscriptionInfo) -> Self {
        let params = match info {
            SubscriptionInfo::Account(acc) => Some(acc.into()),
            SubscriptionInfo::Program(prog) => Some((&**prog).into()),
            SubscriptionInfo::Transaction(trx) => Some(trx.into()),
            SubscriptionInfo::Slot => None,
        };
        Self::new(info.as_str(), params)
    }
}

impl<'a> SubRequest<'a> {
    pub fn new(method: &'a str, params: Option<SubParams<'a>>) -> Self {
        Self {
            id: 0, // will be set later
            jsonrpc: JSONRPC,
            method,
            params,
        }
    }
}

impl<'a> SubConfig<'a> {
    fn new(
        commitment: Commitment,
        encoding: Option<Encoding>,
        filters: Option<&'a Filters>,
    ) -> Self {
        Self {
            commitment,
            encoding,
            filters,
        }
    }
}

impl UnsubRequest {
    pub fn new(id: u64, subscription: u64, method: &'static str) -> Self {
        Self {
            jsonrpc: JSONRPC,
            id,
            method,
            params: [subscription],
        }
    }
}

impl<'a> From<&'a AccountKey> for SubParams<'a> {
    fn from(key: &'a AccountKey) -> Self {
        let config = SubConfig::new(key.commitment, Some(Encoding::Base64Zstd), None);
        let key = EitherKey::Pubkey(key.pubkey.borrow());
        Self { key, config }
    }
}

impl<'a> From<&'a ProgramKey> for SubParams<'a> {
    fn from(key: &'a ProgramKey) -> Self {
        let config = SubConfig::new(
            key.commitment,
            Some(Encoding::Base64Zstd),
            key.filters.as_ref(),
        );
        let key = EitherKey::Pubkey(key.pubkey.borrow());
        Self { key, config }
    }
}

impl<'a> From<&'a TransactionKey> for SubParams<'a> {
    fn from(key: &'a TransactionKey) -> Self {
        let config = SubConfig::new(key.commitment, None, None);
        let key = EitherKey::Signature(key.signature.borrow());
        Self { key, config }
    }
}

impl Display for SubscriptionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Account(_) => write!(f, "account subscription"),
            Self::Program(_) => write!(f, "program subscription"),
            Self::Transaction(_) => write!(f, "transaction subscription"),
            Self::Slot => write!(f, "slot subscription"),
        }
    }
}

impl<'a> Serialize for SubParams<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.key)?;
        seq.serialize_element(&self.config)?;
        seq.end()
    }
}

impl SubscriptionInfo {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Account(_) => ACCOUNT_SUBSCRIBE,
            Self::Program(_) => PROGRAM_SUBSCRIBE,
            Self::Transaction(_) => SIGNATURE_SUBSCRIBE,
            Self::Slot => SLOT_SUBSCRIBE,
        }
    }
}
