use std::{
    collections::{hash_map::Drain, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use tokio::{net::TcpStream, sync::mpsc::Receiver};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Error, Message},
    MaybeTlsStream, WebSocketStream,
};

use crate::{
    cache::InnerCache, metrics::METRICS, types::Commitment, ws::notification::NotificationResult,
};

use super::{
    notification::{NotificationValue as NV, WsMessage},
    subscription::{
        SubRequest, SubscriptionInfo, UnsubRequest, ACCOUNT_UNSUBSCRIBE, PROGRAM_UNSUBSCRIBE,
        SIGNATURE_UNSUBSCRIBE,
    },
};

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

#[derive(Default)]
struct SubscriptionMap {
    id2sub: HashMap<u64, SubscriptionInfo>,
    sub2id: Vec<(SubscriptionInfo, u64)>,
    next: usize,
}

impl SubscriptionMap {
    #[inline]
    fn get(&self, id: &u64) -> Option<&SubscriptionInfo> {
        self.id2sub.get(id)
    }

    #[inline]
    fn insert(&mut self, id: u64, sub: SubscriptionInfo) {
        self.id2sub.insert(id, sub.clone());
        self.sub2id.push((sub, id));
    }

    #[inline]
    fn len(&self) -> usize {
        self.id2sub.len()
    }

    fn remove_current(&mut self) {
        let (_, id) = self.sub2id.swap_remove(self.next.saturating_sub(1));
        self.next = self.next.saturating_sub(1);
        self.id2sub.remove(&id);
    }

    fn remove(&mut self, id: u64) {
        self.id2sub.remove(&id);
        let res = self
            .sub2id
            .iter()
            .enumerate()
            .find(|(_, (_, ident))| ident == &id);
        if let Some((i, _)) = res {
            self.sub2id.swap_remove(i);
        }
    }

    #[inline]
    fn next_to_check(&mut self) -> Option<&(SubscriptionInfo, u64)> {
        let i = self.next;
        if self.next + 1 >= self.sub2id.len() {
            self.next = 0;
        } else {
            self.next += 1;
        }
        self.sub2id.get(i)
    }

    fn drain(&mut self) -> Drain<'_, u64, SubscriptionInfo> {
        self.sub2id.clear();
        self.id2sub.drain()
    }
}

pub(crate) struct WsConnection<U> {
    id: usize,
    subscriptions: SubscriptionMap,
    sub_count: Arc<AtomicU64>,
    inflight: HashMap<u64, SubscriptionInfo>,
    cache: InnerCache,
    rx: Receiver<WsCommand>,
    sink: Sink,
    stream: Stream,
    bytes: Arc<AtomicU64>,
    url: U,
    next: Arc<AtomicU64>,
    name: String,
}

pub(crate) enum WsCommand {
    Subscribe(SubscriptionInfo),
    SlotSubscribe,
}

impl<U> WsConnection<U>
where
    U: IntoClientRequest + Unpin + Clone + Send,
{
    pub(crate) async fn new(
        id: usize,
        url: U,
        rx: Receiver<WsCommand>,
        cache: InnerCache,
        bytes: Arc<AtomicU64>,
        sub_count: Arc<AtomicU64>,
        next: Arc<AtomicU64>,
    ) -> Result<Self, Error> {
        let (ws, _) = connect_async(url.clone()).await?;
        METRICS.active_ws_connections.inc();
        let (sink, stream) = ws.split();
        let subscriptions = SubscriptionMap::default();
        let inflight = HashMap::default();
        let name = format!("webosocket-worker-{}", id);
        let connection = Self {
            id,
            subscriptions,
            sub_count,
            inflight,
            cache,
            rx,
            sink,
            stream,
            bytes,
            url,
            next,
            name,
        };
        Ok(connection)
    }

    pub(crate) async fn run(mut self) {
        loop {
            while let Ok(cmd) = self.rx.try_recv() {
                match cmd {
                    WsCommand::Subscribe(info) => self.subscribe(info).await,
                    WsCommand::SlotSubscribe => self.subscribe(SubscriptionInfo::Slot).await,
                }
            }

            if let Some(entry) = self.subscriptions.next_to_check() {
                let unsubscribe = match entry.0 {
                    SubscriptionInfo::Account(ref key) => {
                        (!self.cache.contains_account(key)).then(|| ACCOUNT_UNSUBSCRIBE)
                    }
                    SubscriptionInfo::Program(ref key) => {
                        (!self.cache.contains_program(key)).then(|| PROGRAM_UNSUBSCRIBE)
                    }
                    SubscriptionInfo::Transaction(ref key) => {
                        (!self.cache.contains_transaction(key)).then(|| SIGNATURE_UNSUBSCRIBE)
                    }
                    SubscriptionInfo::Slot => None,
                };
                let id = entry.1;
                if let Some(method) = unsubscribe {
                    self.subscriptions.remove_current();
                    self.unsubscribe(id, method).await;
                }
            }

            match self.stream.next().await {
                Some(Ok(msg)) => {
                    if self.handle(msg).await {
                        // if handling is ok process next message
                        continue;
                    }
                }
                Some(Err(error)) => {
                    tracing::error!(id=%self.id, %error, "error occured during ws stream processing, reconnecting");
                }
                None => {
                    tracing::warn!(id=%self.id, "webosocket stream terminated, reconnecting");
                }
            }
            METRICS.active_ws_connections.dec();
            self.recreate().await;
        }
    }

    async fn handle(&mut self, msg: Message) -> bool {
        match msg {
            Message::Text(text) => {
                self.bytes.fetch_add(text.len() as u64, Ordering::Relaxed);
                METRICS
                    .ws_data_received
                    .with_label_values(&[&self.name])
                    .inc_by(text.len() as u64);
                match WsMessage::try_from(text.as_str()) {
                    Ok(msg) => self.process_message(msg).await,
                    Err(error) => {
                        tracing::error!(
                            id=%self.id,
                            %error,
                            %text,
                            "error occured during webosocket message parsing",
                        );
                        true
                    }
                }
            }
            Message::Binary(bin) => {
                tracing::warn!(id=%self.id, "unexpected binary message of length: {}", bin.len());
                true
            }
            Message::Ping(msg) => {
                let _ = self.sink.send(Message::Pong(msg)).await;
                true
            }
            Message::Pong(_) => true, // shouldn't happen, as we are not pinging
            Message::Close(reason) => {
                tracing::warn!(
                    id=%self.id,
                    reason=?reason,
                    "webosocket connection aborted, will reconnect",
                );
                false
            }
            Message::Frame(_) => {
                tracing::warn!(id=%self.id, "received incomplete ws frame"); // shouldn't happen, intended for library use
                true
            }
        }
    }

    async fn process_message(&mut self, msg: WsMessage) -> bool {
        match msg {
            WsMessage::SubResult(res) => {
                let info = match self.inflight.remove(&res.id) {
                    Some(v) => v,
                    None => {
                        tracing::warn!("successfully subscribed for non-existent subscription");
                        return true;
                    }
                };
                METRICS
                    .active_subscriptions
                    .with_label_values(&[&self.name])
                    .set(self.subscriptions.len() as i64);
                METRICS
                    .inflight_subscriptions
                    .with_label_values(&[&self.name])
                    .set(self.inflight.len() as i64);
                match info {
                    SubscriptionInfo::Account(_) => {}
                    SubscriptionInfo::Program(_) => {}
                    SubscriptionInfo::Transaction(_) => {}
                    SubscriptionInfo::Slot => {
                        tracing::info!("successfully subscribed for slot updates");
                    }
                };
                self.subscriptions.insert(res.result, info);
            }
            WsMessage::SubError(res) => {
                let info = match self.inflight.remove(&res.id) {
                    Some(v) => v,
                    None => {
                        return true;
                    }
                };
                METRICS
                    .inflight_subscriptions
                    .with_label_values(&[&self.name])
                    .set(self.inflight.len() as i64);
                self.update_sub_count();
                match info {
                    SubscriptionInfo::Account(ref key) => {
                        self.cache.remove_account(key);
                    }
                    SubscriptionInfo::Program(ref key) => {
                        self.cache.remove_program(key);
                    }
                    SubscriptionInfo::Transaction(ref key) => {
                        self.cache.remove_transaction(key);
                    }
                    SubscriptionInfo::Slot => {
                        tracing::error!(id=%self.id, error=%res.error, "coudn't subscribe for slot updates");
                        return false;
                    }
                };
                tracing::error!(id=%self.id, error=%res.error, "error (un)subscribing to ws updates");
            }
            WsMessage::UnsubResult(res) => {
                tracing::debug!(id=%self.id, sub=%res.id, result=%res.result,"unsubscribed from subscription");
            }
            WsMessage::Notification(notification) => {
                let result = match notification.params.result {
                    NotificationResult::Complex(r) => r,
                    NotificationResult::Slot(r) => {
                        self.cache.update_slot(r.slot, Commitment::Processed);
                        self.cache.update_slot(r.parent, Commitment::Confirmed);
                        self.cache.update_slot(r.root, Commitment::Finalized);
                        return true;
                    }
                };

                let id = notification.params.subscription;
                let key = match self.subscriptions.get(&id) {
                    Some(key) => key,
                    None => {
                        tracing::debug!(
                            id=%self.id,
                            "no subscription exists for received notification"
                        );
                        self.unsubscribe(id, ACCOUNT_UNSUBSCRIBE).await;
                        self.unsubscribe(id, PROGRAM_UNSUBSCRIBE).await;
                        return true;
                    }
                };
                match result.value {
                    NV::Account(account) => {
                        let key = match key {
                            SubscriptionInfo::Account(k) => k,
                            _ => {
                                tracing::error!(id=%self.id, "invalid subscription for account");
                                return true;
                            }
                        };
                        if !self.cache.update_account(key.clone(), account.into()) {
                            self.subscriptions.remove(id);
                            self.unsubscribe(id, PROGRAM_UNSUBSCRIBE).await;
                        }
                    }
                    NV::Program(notification) => {
                        let key = match key {
                            SubscriptionInfo::Program(k) => k,
                            _ => {
                                tracing::error!( id=%self.id, "invalid subscription for program");
                                return true;
                            }
                        };
                        if !self.cache.upsert_program_account(notification, key) {
                            self.subscriptions.remove(id);
                            self.unsubscribe(id, PROGRAM_UNSUBSCRIBE).await;
                        }
                    }
                    NV::Transaction(notification) => {
                        let key = match key {
                            SubscriptionInfo::Transaction(k) => k,
                            _ => {
                                tracing::error!( id=%self.id, "invalid subscription for transaction");
                                return true;
                            }
                        };
                        self.cache.update_trasaction(key, notification.into());
                        self.subscriptions.remove(id);
                    }
                }
            }
        }
        true
    }

    async fn recreate(&mut self) {
        let ws = loop {
            METRICS.ws_reconnects.with_label_values(&[&self.name]).inc();
            match connect_async(self.url.clone()).await {
                Ok((ws, _)) => break ws,
                Err(error) => {
                    tracing::error!(%error, "error reconnecting to webosocket");
                    continue;
                }
            }
        };
        METRICS.active_ws_connections.inc();
        let (sink, stream) = ws.split();
        self.sink = sink;
        self.stream = stream;
        self.bytes.store(0, Ordering::Relaxed);
        let mut subscriptions = std::mem::take(&mut self.subscriptions);
        let mut inflight = std::mem::take(&mut self.inflight);
        let subs = subscriptions
            .drain()
            .chain(inflight.drain())
            .map(|(_, s)| s);

        for s in subs {
            match s {
                SubscriptionInfo::Account(key) => {
                    self.cache.remove_account(&key);
                }
                SubscriptionInfo::Program(key) => {
                    self.cache.remove_program(&key);
                }
                SubscriptionInfo::Transaction(key) => {
                    self.cache.remove_transaction(&key);
                }
                info @ SubscriptionInfo::Slot => self.subscribe(info).await,
            }
        }
        METRICS
            .active_subscriptions
            .with_label_values(&[&self.name])
            .set(self.subscriptions.len() as i64);
        METRICS
            .inflight_subscriptions
            .with_label_values(&[&self.name])
            .set(self.inflight.len() as i64);
        self.update_sub_count();
    }

    async fn subscribe(&mut self, info: SubscriptionInfo) {
        self.update_sub_count();
        tracing::debug!(id=%self.id, %info, "subscribed for updates");
        let mut request: SubRequest<'_> = (&info).into();
        request.id = self.next_request_id();
        let msg = Message::Text(json::to_string(&request).unwrap());
        self.inflight.insert(request.id, info);
        METRICS
            .inflight_subscriptions
            .with_label_values(&[&self.name])
            .set(self.inflight.len() as i64);
        let _ = self.sink.send(msg).await;
    }

    async fn unsubscribe(&mut self, subscription: u64, method: &'static str) {
        let id = self.next_request_id();
        METRICS
            .active_subscriptions
            .with_label_values(&[&self.name])
            .set(self.subscriptions.len() as i64);
        METRICS.unsubscriptions.with_label_values(&[method]).inc();
        self.update_sub_count();
        let request = UnsubRequest::new(id, subscription, method);
        let msg = Message::Text(json::to_string(&request).unwrap());

        let _ = self.sink.send(msg).await;
    }

    #[inline]
    fn update_sub_count(&self) {
        let count = (self.subscriptions.len() + self.inflight.len()) as u64;
        self.sub_count.store(count, Ordering::Relaxed);
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next.load(Ordering::Relaxed);
        self.next.fetch_add(1, Ordering::Relaxed);
        id
    }
}
