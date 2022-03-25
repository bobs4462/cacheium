use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use dashmap::DashMap;
use futures_delay_queue::{DelayHandle, DelayQueue as FutureDelayQueue, Receiver};
use futures_intrusive::buffer::GrowingHeapBuf;
use serde::{ser::SerializeSeq, Serialize};
use smallvec::SmallVec;
use tokio::sync::mpsc::Sender;
use ws::connection::WsHandler;

type DelayQueue<T> = Arc<Mutex<FutureDelayQueue<T, GrowingHeapBuf<T>>>>;

#[derive(Clone)]
struct InnerCache<'a> {
    accounts: Arc<DashMap<AccountKey<'a>, u64>>,
    programs: Arc<DashMap<ProgramKey<'a>, u64>>,
}

#[derive(Clone)]
pub struct Cache<'a> {
    inner: InnerCache<'a>,
    arx: Receiver<AccountKey<'static>>,
    prx: Receiver<ProgramKey<'static>>,
    aqueue: DelayQueue<AccountKey<'static>>,
    pqueue: DelayQueue<ProgramKey<'static>>,
}

pub struct CachValue<T> {
    value: T,
    handle: DelayHandle,
    sub: Subscription,
}

pub struct WsConnectionManager {
    connections: Arc<HashMap<usize, WsHandler>>,
    loads: Vec<WsLoad>,
}

pub struct WsLoad {
    id: usize,
    bytes: u64,
}

pub enum Subscription {
    Account(SubMeta),
    Program(SubMeta),
}

pub struct SubMeta {
    id: u64,
    connection: usize,
}

pub struct Account {
    owner: Pubkey,
    data: Vec<u8>,
    lamports: u64,
    executable: bool,
    rent_epoch: u64,
}

pub struct ProgramAccounts(HashSet<Pubkey>);

#[derive(Clone, Eq, Hash, PartialEq)]
struct AccountKey<'a> {
    pubkey: Cow<'a, Pubkey>,
    commmitment: Cow<'a, Commitment>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct Filters<'a>(Cow<'a, SmallVec<[ProgramFilter; 3]>>);

#[derive(Clone, Eq, Hash, PartialEq)]
struct ProgramKey<'a> {
    pubkey: Cow<'a, Pubkey>,
    commmitment: Cow<'a, Commitment>,
    filters: Option<Filters<'a>>,
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize)]
enum ProgramFilter {
    DataSize(u64),
    Memcmp(MemcmpFilter),
}

#[derive(Clone, Eq, Hash, PartialEq, Serialize)]
struct MemcmpFilter {
    offset: u64,
    bytes: Pattern,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct Pattern(SmallVec<[u8; 64]>);

#[derive(Clone, Eq, Hash, PartialEq)]
struct Pubkey([u8; 32]);

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

const MAX_BASE58_PATTERN: usize = 175;
impl Serialize for Pattern {
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

impl<'a> Serialize for Filters<'a> {
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

#[derive(Clone, Eq, Hash, PartialEq, Serialize, Copy)]
enum Commitment {
    Processed,
    Confirmed,
    Finalized,
}
#[derive(Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    Base58,
    Base64,
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
}

mod ws;
