use std::{
    collections::HashMap,
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
        SubMeta, SubRequest, SubscriptionInfo, UnsubRequest, ACCOUNT_UNSUBSCRIBE,
        PROGRAM_UNSUBSCRIBE,
    },
};

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub(crate) struct WsConnection<U> {
    id: usize,
    subscriptions: HashMap<u64, SubscriptionInfo>,
    sub_count: Arc<AtomicU64>,
    inflight: HashMap<u64, SubscriptionInfo>,
    cache: InnerCache,
    rx: Receiver<WsCommand>,
    sink: Sink,
    stream: Stream,
    bytes: Arc<AtomicU64>,
    url: U,
    next: u64,
    name: String,
}

pub(crate) enum WsCommand {
    Subscribe(SubscriptionInfo),
    SlotSubscribe,
    Unsubscribe(u64),
    #[allow(unused)]
    Connect,
    #[allow(unused)]
    Disconnect,
    #[allow(unused)]
    Reconnect,
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
    ) -> Result<Self, Error> {
        let (ws, _) = connect_async(url.clone()).await?;
        METRICS.active_ws_connections.inc();
        let (sink, stream) = ws.split();
        let subscriptions = HashMap::default();
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
            next: 0,
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
                    WsCommand::Unsubscribe(id) => self.unsubscribe(id).await,
                    WsCommand::Connect => todo!(),
                    WsCommand::Disconnect => todo!(),
                    WsCommand::Reconnect => todo!(),
                }
            }

            match self.stream.next().await {
                Some(Ok(msg)) => {
                    if self.handle(msg) {
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

    fn handle(&mut self, msg: Message) -> bool {
        match msg {
            Message::Text(text) => {
                self.bytes.fetch_add(text.len() as u64, Ordering::Relaxed);
                METRICS
                    .ws_data_received
                    .with_label_values(&[&self.name])
                    .inc_by(text.len() as u64);
                match WsMessage::try_from(text.as_str()) {
                    Ok(msg) => self.process_message(msg),
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
                tracing::warn!(id=%self.id, "unexpected binary message of lenght: {}", bin.len());
                true
            }
            Message::Ping(_) => true, // library already handled the pong
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

    fn process_message(&mut self, msg: WsMessage) -> bool {
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
                    .with_label_values(&[&self.name, info.as_str()])
                    .inc();
                METRICS
                    .inflight_subscriptions
                    .with_label_values(&[&self.name, info.as_str()])
                    .set(self.inflight.len() as i64);
                match info {
                    SubscriptionInfo::Account(ref key) => {
                        let meta = SubMeta::new(res.result, self.id);
                        self.cache.update_account_meta(key, meta);
                    }
                    SubscriptionInfo::Program(ref key) => {
                        let meta = SubMeta::new(res.result, self.id);
                        self.cache.update_program_meta(key, meta);
                    }
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
                        tracing::warn!("got error for non-existent subscription");
                        return true;
                    }
                };
                METRICS
                    .inflight_subscriptions
                    .with_label_values(&[&self.name, info.as_str()])
                    .set(self.inflight.len() as i64);
                self.update_sub_count();
                match info {
                    SubscriptionInfo::Account(ref key) => {
                        self.cache.remove_account(key);
                    }
                    SubscriptionInfo::Program(ref key) => {
                        self.cache.remove_program(key);
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
                    NotificationResult::Account(r) => r,
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
                        tracing::warn!(
                            id=%self.id,
                            subs=?self.subscriptions.keys(),
                            "no subscription exists for received notification"
                        );
                        return true;
                    }
                };
                match result.value {
                    NV::Account(account) => {
                        let key = match key {
                            SubscriptionInfo::Account(k) => k,
                            _ => {
                                tracing::error!(id=%self.id, "invalid subscription for account, shouldn't happen");
                                return true;
                            }
                        };
                        self.cache.update_account(key, account.into());
                    }
                    NV::Program(notification) => {
                        let key = match key {
                            SubscriptionInfo::Program(k) => k,
                            _ => {
                                tracing::error!(
                                    id=%self.id,
                                    "invalid subscription for program, shouldn't happen"
                                );
                                return true;
                            }
                        };
                        self.cache.upsert_program_account(notification, key);
                    }
                }
            }
        }
        true
    }

    async fn recreate(&mut self) {
        let (ws, _) = connect_async(self.url.clone()).await.unwrap();
        METRICS.active_ws_connections.inc();
        METRICS.ws_reconnects.with_label_values(&[&self.name]).inc();
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
            METRICS
                .active_subscriptions
                .with_label_values(&[&self.name, s.as_str()])
                .dec();
            match s {
                SubscriptionInfo::Account(key) => {
                    self.cache.remove_account(&key);
                }
                SubscriptionInfo::Program(key) => {
                    self.cache.remove_program(&key);
                }
                info @ SubscriptionInfo::Slot => self.subscribe(info).await,
            }
        }
        self.update_sub_count();
    }

    async fn subscribe(&mut self, info: SubscriptionInfo) {
        tracing::debug!(id=%self.id, %info, "subscribed for updates");
        let mut request: SubRequest<'_> = (&info).into();
        request.id = self.next_request_id();
        let msg = Message::Text(json::to_string(&request).unwrap());
        let inflight_metrics = METRICS
            .inflight_subscriptions
            .with_label_values(&[&self.name, info.as_str()]);
        self.inflight.insert(request.id, info);
        inflight_metrics.set(self.inflight.len() as i64);
        self.sink.send(msg).await.unwrap();
        self.update_sub_count();
    }

    async fn unsubscribe(&mut self, subscription: u64) {
        tracing::debug!(id=%self.id, %subscription, "unsubscribing from sub");
        let info = match self.subscriptions.remove(&subscription) {
            Some(info) => info,
            None => {
                tracing::warn!(id=%self.id, "unsubscription request from non-existent sub");
                return;
            }
        };
        let id = self.next_request_id();
        METRICS
            .active_subscriptions
            .with_label_values(&[&self.name, info.as_str()])
            .set(self.subscriptions.len() as i64);
        self.update_sub_count();
        let method = match info {
            SubscriptionInfo::Account(_) => ACCOUNT_UNSUBSCRIBE,
            SubscriptionInfo::Program(_) => PROGRAM_UNSUBSCRIBE,
            SubscriptionInfo::Slot => {
                tracing::warn!("trying to unsubscribe from slot updates, not allowed");
                return;
            }
        };
        let request = UnsubRequest::new(id, subscription, method);
        let msg = Message::Text(json::to_string(&request).unwrap());

        self.sink.send(msg).await.unwrap();
    }

    #[inline]
    fn update_sub_count(&self) {
        let count = (self.subscriptions.len() + self.inflight.len()) as u64;
        self.sub_count.store(count, Ordering::Relaxed);
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next;
        self.next += 1;
        id
    }
}
