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
    tungstenite::{client::IntoClientRequest, Message},
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

impl<U: IntoClientRequest + Unpin + Clone + Send> WsConnection<U> {
    pub async fn new(
        id: usize,
        url: U,
        rx: Receiver<WsCommand>,
        cache: InnerCache,
        bytes: Arc<AtomicU64>,
    ) -> Self {
        let (ws, _) = connect_async(url.clone()).await.unwrap();
        let (sink, stream) = ws.split();
        let subscriptions = HashMap::default();
        let inflight = HashMap::default();
        Self {
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
        }
    }

    pub async fn run(mut self) {
        loop {
            while let Ok(cmd) = self.rx.try_recv() {
                match cmd {
                    WsCommand::Subscribe(info) => self.subscribe(&info).await,
                    WsCommand::Unsubscribe(id) => self.unsubscribe(id).await,
                    WsCommand::Connect => todo!(),
                    WsCommand::Disconnect => todo!(),
                    WsCommand::Reconnect => todo!(),
                }
            }

            match self.stream.next().await {
                Some(Ok(msg)) => {
                    if self.handle(msg) {
                        continue;
                    }
                }
                Some(Err(err)) => {
                    println!("error occured during ws stream processing: {}", err);
                }
                None => {
                    println!("webosocket stream terminated");
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
                        println!("error occured during webosocket message parsing: {}", error);
                    }
                }
            }
            Message::Binary(bin) => println!("unexpected binary message of len: {}", bin.len()),
            Message::Ping(_) => (), // library already handled the pong
            Message::Pong(_) => (), // shouldn't happen, as we are not pinging
            Message::Close(reason) => {
                println!(
                    "webosocket connection {} aborted, reconnecting: {:?}",
                    self.id, reason
                );
                return false;
            }
            Message::Frame(_) => (), // shouldn't happen, intended for library use
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
                        if let Some(mut v) = self.cache.accounts.get_mut(key) {
                            let meta = SubMeta::new(res.result, self.id);
                            v.set_subscription(meta);
                        }
                    }
                    SubscriptionInfo::Program(ref key) => {
                        if let Some(mut v) = self.cache.programs.get_mut(key) {
                            let meta = SubMeta::new(res.result, self.id);
                            v.sub.replace(meta);
                        }
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
                        self.cache.accounts.remove(key);
                    }
                    SubscriptionInfo::Program(ref key) => {
                        self.cache.programs.remove(key);
                    }
                };
                println!("error (un)subscribing to ws updates");
            }
            WsMessage::UnsubResult(res) => {
                println!("unsubscribed from {}", res.id);
            }
            WsMessage::Notification(notification) => {
                use NotificationMethod::*;
                let id = notification.params.subscription;
                let key = match self.subscriptions.get(&id) {
                    Some(key) => key,
                    None => {
                        println!("error, no sub exists");
                        return;
                    }
                };
                match (notification.method, notification.params.result.value) {
                    (AccountNotification, NV::Account(account)) => {
                        let key = match key {
                            SubscriptionInfo::Account(k) => k,
                            SubscriptionInfo::Program(_) => {
                                println!("program subscription for account, shouldn't happen");
                                return;
                            }
                        };
                        if let Some(mut v) = self.cache.accounts.get_mut(key) {
                            v.set_value(account);
                        }
                    }
                    (ProgramNotification, NV::Program(notification)) => {
                        let key = match key {
                            SubscriptionInfo::Account(_) => {
                                println!("program subscription for account, shouldn't happen");
                                return;
                            }
                            SubscriptionInfo::Program(k) => k,
                        };
                        self.cache.upsert_program_account(notification, key);
                    }
                    _ => println!("error"), // TODO shouldn't happen
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
                    self.cache.accounts.remove(&key);
                }
                SubscriptionInfo::Program(key) => {
                    self.cache.programs.remove(&key);
                }
            }
        }
    }

    async fn subscribe(&mut self, info: &SubscriptionInfo) {
        let mut request: SubRequest = info.into();
        request.id = self.next_request_id();
        let msg = Message::Text(json::to_string(&request).unwrap());
        self.sink.send(msg).await.unwrap();
    }

    async fn unsubscribe(&mut self, subscription: u64) {
        let info = match self.subscriptions.remove(&subscription) {
            Some(info) => info,
            None => {
                println!("unsubscription request from non-existent sub");
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
