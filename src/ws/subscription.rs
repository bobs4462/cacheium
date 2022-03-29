use std::borrow::Borrow;

use serde::ser::SerializeSeq;
use serde::Serialize;

use crate::types::{AccountKey, Commitment, Encoding, Filters, ProgramKey, Pubkey};

const JSONRPC: &str = "2.0";
const ACCOUNT_SUBSCRIBE: &str = "accountSubscribe";
pub const ACCOUNT_UNSUBSCRIBE: &str = "accountUnsubscribe";
const PROGRAM_SUBSCRIBE: &str = "programSubscribe";
pub const PROGRAM_UNSUBSCRIBE: &str = "programUnsubscribe";

pub enum SubscriptionInfo {
    Account(AccountKey),
    Program(ProgramKey),
}

#[derive(Serialize)]
pub struct SubRequest<'a> {
    jsonrpc: &'a str,
    pub id: u64,
    method: &'a str,
    params: SubParams<'a>,
}

#[derive(Serialize)]
pub struct UnsubRequest {
    jsonrpc: &'static str,
    id: u64,
    method: &'static str,
    params: [u64; 1],
}

pub struct SubParams<'a> {
    pubkey: &'a Pubkey,
    config: SubConfig<'a>,
}

#[derive(Serialize)]
pub struct SubConfig<'a> {
    commitment: Commitment,
    encoding: Encoding,
    filters: Option<&'a Filters>,
}

pub struct SubMeta {
    pub id: u64,
    pub connection: usize,
}

impl<'a> From<&'a SubscriptionInfo> for SubRequest<'a> {
    fn from(info: &'a SubscriptionInfo) -> Self {
        let params = match info {
            SubscriptionInfo::Account(acc) => acc.into(),
            SubscriptionInfo::Program(prog) => prog.into(),
        };
        Self {
            id: 0, // will be set later
            jsonrpc: JSONRPC,
            method: info.as_str(),
            params,
        }
    }
}

impl<'a> SubConfig<'a> {
    fn new(commitment: Commitment, encoding: Encoding, filters: Option<&'a Filters>) -> Self {
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

impl SubMeta {
    pub fn new(id: u64, connection: usize) -> Self {
        Self { id, connection }
    }
}

impl<'a> From<&'a AccountKey> for SubParams<'a> {
    fn from(key: &'a AccountKey) -> Self {
        let config = SubConfig::new(key.commitment, Encoding::Base64Zstd, None);
        let pubkey = key.pubkey.borrow();
        Self { pubkey, config }
    }
}

impl<'a> From<&'a ProgramKey> for SubParams<'a> {
    fn from(key: &'a ProgramKey) -> Self {
        let config = SubConfig::new(key.commitment, Encoding::Base64Zstd, key.filters.as_ref());
        let pubkey = key.pubkey.borrow();
        Self { pubkey, config }
    }
}

impl<'a> Serialize for SubParams<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.pubkey)?;
        seq.serialize_element(&self.config)?;
        seq.end()
    }
}

impl SubscriptionInfo {
    fn as_str(&self) -> &str {
        match self {
            Self::Account(_) => ACCOUNT_SUBSCRIBE,
            Self::Program(_) => PROGRAM_SUBSCRIBE,
        }
    }
}
