use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::sync::mpsc::{channel, Sender};
use tokio_tungstenite::tungstenite::Error;

use crate::cache::InnerCache;

use super::{
    connection::{WsCommand, WsConnection},
    subscription::{SubMeta, SubscriptionInfo},
};

#[derive(Clone)]
pub(crate) struct WsConnectionManager {
    connections: Arc<HashMap<usize, Sender<WsCommand>>>,
    load_distribution: Arc<Vec<WsLoad>>,
}

pub(crate) struct WsLoad {
    id: usize,
    bytes: Arc<AtomicU64>,
    subs: Arc<AtomicU64>,
}

impl PartialEq for WsLoad {
    fn eq(&self, other: &Self) -> bool {
        let ss = self.subs.load(Ordering::Relaxed);
        let os = other.subs.load(Ordering::Relaxed);
        let sb = self.bytes.load(Ordering::Relaxed);
        let ob = other.bytes.load(Ordering::Relaxed);
        ss == os && sb == ob
    }
}

impl PartialOrd for WsLoad {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        let ss = self.subs.load(Ordering::Relaxed);
        let os = other.subs.load(Ordering::Relaxed);
        let sb = self.bytes.load(Ordering::Relaxed);
        let ob = other.bytes.load(Ordering::Relaxed);
        Some(sb.cmp(&ob).then(ss.cmp(&os)))
    }
}

impl Eq for WsLoad {}
impl Ord for WsLoad {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let ss = self.subs.load(Ordering::Relaxed);
        let os = other.subs.load(Ordering::Relaxed);
        let sb = self.bytes.load(Ordering::Relaxed);
        let ob = other.bytes.load(Ordering::Relaxed);
        sb.cmp(&ob).then(ss.cmp(&os))
    }
}

impl WsLoad {
    fn new(id: usize, bytes: Arc<AtomicU64>, subs: Arc<AtomicU64>) -> Self {
        Self { id, bytes, subs }
    }
}

impl WsConnectionManager {
    pub(crate) async fn new(url: String, cons: usize, cache: InnerCache) -> Result<Self, Error> {
        let mut connections = HashMap::with_capacity(cons);
        let mut load_distribution = Vec::with_capacity(cons);
        for id in 0..cons {
            let (tx, load) = Self::connect(id, cache.clone(), url.clone()).await?;
            connections.insert(id, tx);
            load_distribution.push(load);
        }
        // create a separate connection for slot updates
        let (tx, _) = Self::connect(usize::MAX, cache, url).await?;
        let _ = tx.send(WsCommand::SlotSubscribe).await;
        let manager = Self {
            connections: Arc::new(connections),
            load_distribution: Arc::new(load_distribution),
        };
        Ok(manager)
    }

    async fn connect(
        id: usize,
        cache: InnerCache,
        url: String,
    ) -> Result<(Sender<WsCommand>, WsLoad), Error> {
        let bytes = Arc::default();
        let subs = Arc::default();
        let (tx, rx) = channel(1024);
        let connection =
            WsConnection::new(id, url, rx, cache, Arc::clone(&bytes), Arc::clone(&subs)).await?;
        let load = WsLoad::new(id, bytes, subs);
        tokio::spawn(connection.run());
        Ok((tx, load))
    }

    pub(crate) fn subscribe(&self, info: SubscriptionInfo) {
        let ws = self.load_distribution.iter().min().unwrap();
        let tx = self.connections.get(&ws.id).unwrap();
        let cmd = WsCommand::Subscribe(info);
        if let Err(error) = tx.try_send(cmd) {
            tracing::error!(%error, "failed to create subscription request");
        }
    }
    pub(crate) fn unsubscribe(&self, submeta: SubMeta) {
        let tx = self.connections.get(&submeta.connection).unwrap();
        let cmd = WsCommand::Unsubscribe(submeta.id);
        if let Err(error) = tx.try_send(cmd) {
            tracing::error!(id=%submeta.id, %error, "failed to create unsubscription request");
        }
    }
}
