use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::sync::mpsc::{channel, Sender};
use tokio_tungstenite::tungstenite::Error;

use crate::cache::InnerCache;

use super::{
    connection::{WsCommand, WsConnection},
    subscription::{SubMeta, SubscriptionInfo},
};

#[derive(Clone)]
pub struct WsConnectionManager {
    connections: Arc<HashMap<usize, Sender<WsCommand>>>,
    load_distribution: Arc<Vec<WsLoad>>,
}

pub struct WsLoad {
    id: usize,
    bytes: Arc<AtomicU64>,
}

impl PartialEq for WsLoad {
    fn eq(&self, other: &Self) -> bool {
        let s = self.bytes.load(Ordering::Relaxed);
        let o = other.bytes.load(Ordering::Relaxed);
        s == o
    }
}

impl PartialOrd for WsLoad {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let s = self.bytes.load(Ordering::Relaxed);
        let o = other.bytes.load(Ordering::Relaxed);
        s.partial_cmp(&o)
    }
}

impl Eq for WsLoad {}
impl Ord for WsLoad {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let s = self.bytes.load(Ordering::Relaxed);
        let o = other.bytes.load(Ordering::Relaxed);
        s.cmp(&o)
    }
}

impl WsLoad {
    fn new(id: usize, bytes: Arc<AtomicU64>) -> Self {
        Self { id, bytes }
    }
}

impl WsConnectionManager {
    pub async fn new(url: String, cons: usize, cache: InnerCache) -> Result<Self, Error> {
        let mut connections = HashMap::with_capacity(cons);
        let mut load_distribution = Vec::with_capacity(cons);
        for id in 0..cons {
            let (tx, load) = Self::connection(id, cache.clone(), url.clone()).await?;
            connections.insert(id, tx);
            load_distribution.push(load);
        }
        // create a separate connection for slot updates
        let (tx, _) = Self::connection(usize::MAX, cache, url).await?;
        let _ = tx.send(WsCommand::SlotSubscribe).await;
        let manager = Self {
            connections: Arc::new(connections),
            load_distribution: Arc::new(load_distribution),
        };
        Ok(manager)
    }

    pub async fn connection(
        id: usize,
        cache: InnerCache,
        url: String,
    ) -> Result<(Sender<WsCommand>, WsLoad), Error> {
        let bytes = Arc::default();
        let (tx, rx) = channel(512);
        let connection = WsConnection::new(id, url, rx, cache, Arc::clone(&bytes)).await?;
        let load = WsLoad::new(id, bytes);
        tokio::spawn(connection.run());
        Ok((tx, load))
    }

    pub async fn subscribe(&self, info: SubscriptionInfo) {
        let ws = self.load_distribution.iter().min().unwrap();
        let tx = self.connections.get(&ws.id).unwrap();
        let cmd = WsCommand::Subscribe(info);
        let _ = tx.send(cmd).await;
    }
    pub async fn unsubscribe(&self, submeta: SubMeta) {
        let tx = self.connections.get(&submeta.connection).unwrap();
        let cmd = WsCommand::Unsubscribe(submeta.id);
        let _ = tx.send(cmd).await;
    }
}
