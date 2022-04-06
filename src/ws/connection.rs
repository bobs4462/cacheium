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

use crate::cache::InnerCache;

use super::{
    notification::{NotificationMethod, NotificationValue as NV, WsMessage},
    subscription::{
        SubMeta, SubRequest, SubscriptionInfo, UnsubRequest, ACCOUNT_UNSUBSCRIBE,
        PROGRAM_UNSUBSCRIBE,
    },
};

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub struct WsConnection<U> {
    id: usize,
    subscriptions: HashMap<u64, SubscriptionInfo>,
    inflight: HashMap<u64, SubscriptionInfo>,
    cache: InnerCache,
    rx: Receiver<WsCommand>,
    sink: Sink,
    stream: Stream,
    bytes: Arc<AtomicU64>,
    url: U,
    next: u64,
}

pub enum WsCommand {
    Subscribe(SubscriptionInfo),
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
    pub async fn new(
        id: usize,
        url: U,
        rx: Receiver<WsCommand>,
        cache: InnerCache,
        bytes: Arc<AtomicU64>,
    ) -> Result<Self, Error> {
        let (ws, _) = connect_async(url.clone()).await?;
        let (sink, stream) = ws.split();
        let subscriptions = HashMap::default();
        let inflight = HashMap::default();
        let connection = Self {
            id,
            subscriptions,
            inflight,
            cache,
            rx,
            sink,
            stream,
            bytes,
            url,
            next: 0,
        };
        Ok(connection)
    }

    pub async fn run(mut self) {
        loop {
            while let Ok(cmd) = self.rx.try_recv() {
                match cmd {
                    WsCommand::Subscribe(info) => self.subscribe(info).await,
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
            self.recreate().await;
        }
    }

    fn handle(&mut self, msg: Message) -> bool {
        match msg {
            Message::Text(text) => {
                self.bytes.fetch_add(text.len() as u64, Ordering::Relaxed);
                match WsMessage::try_from(text) {
                    Ok(msg) => self.process_message(msg),
                    Err(error) => {
                        tracing::error!(
                            id=%self.id,
                            %error,
                            "error occured during webosocket message parsing",
                        );
                    }
                }
            }
            Message::Binary(bin) => {
                tracing::warn!(id=%self.id, "unexpected binary message of lenght: {}", bin.len())
            }
            Message::Ping(_) => (), // library already handled the pong
            Message::Pong(_) => (), // shouldn't happen, as we are not pinging
            Message::Close(reason) => {
                tracing::warn!(
                    id=%self.id,
                    reason=?reason,
                    "webosocket connection aborted, will reconnect",
                );
                return false;
            }
            Message::Frame(_) => tracing::warn!(id=%self.id, "received incomplete ws frame"), // shouldn't happen, intended for library use
        }
        true
    }

    fn process_message(&mut self, msg: WsMessage) {
        match msg {
            WsMessage::SubResult(res) => {
                let info = match self.inflight.remove(&res.id) {
                    Some(v) => v,
                    None => return,
                };
                match info {
                    SubscriptionInfo::Account(ref key) => {
                        let meta = SubMeta::new(res.result, self.id);
                        self.cache.update_account_meta(key, meta);
                    }
                    SubscriptionInfo::Program(ref key) => {
                        let meta = SubMeta::new(res.result, self.id);
                        self.cache.update_program_meta(key, meta);
                    }
                };
                self.subscriptions.insert(res.result, info);
            }
            WsMessage::SubError(res) => {
                let info = match self.inflight.remove(&res.id) {
                    Some(v) => v,
                    None => return,
                };
                match info {
                    SubscriptionInfo::Account(ref key) => {
                        self.cache.remove_account(key);
                    }
                    SubscriptionInfo::Program(ref key) => {
                        self.cache.remove_program(key);
                    }
                };
                tracing::error!(id=%self.id, error=%res.error, "error (un)subscribing to ws updates");
            }
            WsMessage::UnsubResult(res) => {
                tracing::info!(id=%self.id, sub=%res.id, result=%res.result,"unsubscribed from subscription");
            }
            WsMessage::Notification(notification) => {
                use NotificationMethod::*;
                let id = notification.params.subscription;
                let key = match self.subscriptions.get(&id) {
                    Some(key) => key,
                    None => {
                        tracing::warn!(id=%self.id, subs=?self.subscriptions.keys(), "no subscription exists for received notification");
                        return;
                    }
                };
                let slot = notification.params.result.context.slot;
                tracing::info!(%key, %id, subs=?self.subscriptions.keys(), "got notification for sub");
                match (notification.method, notification.params.result.value) {
                    (AccountNotification, NV::Account(account)) => {
                        let key = match key {
                            SubscriptionInfo::Account(k) => k,
                            SubscriptionInfo::Program(_) => {
                                tracing::error!(id=%self.id, "program subscription for account, shouldn't happen");
                                return;
                            }
                        };
                        self.cache.update_account(key, account.into(), slot);
                    }
                    (ProgramNotification, NV::Program(notification)) => {
                        let key = match key {
                            SubscriptionInfo::Account(_) => {
                                tracing::error!(
                                    id=%self.id,
                                    "account subscription for program, shouldn't happen"
                                );
                                return;
                            }
                            SubscriptionInfo::Program(k) => k,
                        };
                        self.cache.upsert_program_account(notification, key, slot);
                    }
                    _ => tracing::error!("received garbage from webosocket server"), // shouldn't happen
                }
            }
        }
    }

    async fn recreate(&mut self) {
        let (ws, _) = connect_async(self.url.clone()).await.unwrap();
        let (sink, stream) = ws.split();
        self.sink = sink;
        self.stream = stream;
        let subs = self
            .subscriptions
            .drain()
            .chain(self.inflight.drain())
            .map(|(_, s)| s);

        for s in subs {
            match s {
                SubscriptionInfo::Account(key) => {
                    self.cache.remove_account(&key);
                }
                SubscriptionInfo::Program(key) => {
                    self.cache.remove_program(&key);
                }
            }
        }
    }

    async fn subscribe(&mut self, info: SubscriptionInfo) {
        tracing::info!(id=%self.id, %info, "subscribed from sub");
        let mut request: SubRequest<'_> = (&info).into();
        request.id = self.next_request_id();
        let msg = Message::Text(json::to_string(&request).unwrap());
        self.inflight.insert(request.id, info);
        self.sink.send(msg).await.unwrap();
    }

    async fn unsubscribe(&mut self, subscription: u64) {
        tracing::info!(id=%self.id, %subscription, "unsubscribing from sub");
        let info = match self.subscriptions.remove(&subscription) {
            Some(info) => info,
            None => {
                tracing::warn!(id=%self.id, "unsubscription request from non-existent sub");
                return;
            }
        };
        let id = self.next_request_id();
        let method = match info {
            SubscriptionInfo::Account(_) => ACCOUNT_UNSUBSCRIBE,
            SubscriptionInfo::Program(_) => PROGRAM_UNSUBSCRIBE,
        };
        let request = UnsubRequest::new(id, subscription, method);
        let msg = Message::Text(json::to_string(&request).unwrap());

        self.sink.send(msg).await.unwrap();
    }

    fn next_request_id(&mut self) -> u64 {
        let id = self.next;
        self.next += 1;
        id
    }
}
