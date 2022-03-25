use std::borrow::Borrow;

use serde::ser::SerializeSeq;
use serde::Serialize;

use crate::AccountKey;
use crate::Commitment;
use crate::Encoding;
use crate::Filters;
use crate::ProgramKey;
use crate::Pubkey;

const JSONRPC: &str = "2.0";
const ACCOUNT_SUBSCRIBE: &str = "accountSubscribe";
const PROGRAM_SUBSCRIBE: &str = "programSubscribe";

pub enum SubscriptionInfo {
    Account(AccountKey<'static>),
    Program(ProgramKey<'static>),
}

#[derive(Serialize)]
pub struct SubRequest<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    params: SubParams<'a>,
}

pub struct SubParams<'a> {
    pubkey: &'a Pubkey,
    config: SubConfig<'a>,
}

#[derive(Serialize)]
pub struct SubConfig<'a> {
    commitment: Commitment,
    encoding: Encoding,
    filters: Option<&'a Filters<'a>>,
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
    fn new(commitment: Commitment, encoding: Encoding, filters: Option<&'a Filters<'a>>) -> Self {
        Self {
            commitment,
            encoding,
            filters,
        }
    }
}

impl<'a> From<&'a AccountKey<'static>> for SubParams<'a> {
    fn from(key: &'a AccountKey<'static>) -> Self {
        let config = SubConfig::new(key.commmitment.into_owned(), Encoding::Base64Zstd, None);
        let pubkey = key.pubkey.borrow();
        Self { pubkey, config }
    }
}

impl<'a> From<&'a ProgramKey<'static>> for SubParams<'a> {
    fn from(key: &'a ProgramKey<'static>) -> Self {
        let config = SubConfig::new(
            key.commmitment.into_owned(),
            Encoding::Base64Zstd,
            key.filters.as_ref(),
        );
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
