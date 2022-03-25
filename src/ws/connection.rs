use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use futures_util::{
    stream::{SplitSink, SplitStream},
    StreamExt,
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
    MaybeTlsStream, WebSocketStream,
};

use crate::InnerCache;

use super::subscription::SubscriptionInfo;

type Sink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type Stream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub struct WsHandler {
    tx: Sender<SubCommand>,
}

pub struct WsConnection<U> {
    subscriptions: HashMap<u64, SubscriptionInfo>,
    cache: InnerCache<'static>,
    rx: Receiver<SubCommand>,
    sink: Sink,
    stream: Stream,
    bytes: Arc<AtomicU64>,
    url: U,
}

enum SubCommand {
    Subscribe(SubscriptionInfo),
    Unsubscribe(u64),
    Connect,
    Disconnect,
    Reconnect,
}

impl<U: IntoClientRequest + Unpin + Clone> WsConnection<U> {
    async fn new(
        url: U,
        rx: Receiver<SubCommand>,
        cache: InnerCache<'static>,
        bytes: Arc<AtomicU64>,
    ) -> Self {
        let (ws, _) = connect_async(url.clone()).await.unwrap();
        let (sink, stream) = ws.split();
        let subscriptions = HashMap::default();
        Self {
            subscriptions,
            cache,
            rx,
            sink,
            stream,
            bytes,
            url,
        }
    }

    async fn run(mut self) {
        loop {
            while let Ok(cmd) = self.rx.try_recv() {
                match cmd {
                    SubCommand::Subscribe(info) => self.subscribe(info).await,
                    SubCommand::Unsubscribe(id) => self.unsubscribe(id).await,
                    SubCommand::Connect => todo!(),
                    SubCommand::Disconnect => todo!(),
                    SubCommand::Reconnect => todo!(),
                }
            }

            match self.stream.next().await {
                Some(Ok(msg)) => (),
                Some(Err(err)) => todo!(),
                None => (),
            }
        }
    }

    async fn handle(&mut self, msg: Message) {
        match msg {
            Message::Text(text) => todo!(),
            Message::Binary(bin) => todo!(),
            Message::Ping(_) => (), // library already handled the pong
            Message::Pong(_) => (), // shouldn't happen, as we are not pinging
            Message::Close(reason) => todo!(),
            Message::Frame(_) => (), // shouldn't happen, intended for library use
        }
    }

    async fn recreate(self) {}

    async fn subscribe(&self, info: SubscriptionInfo) {}
    async fn unsubscribe(&self, id: u64) {}

    async fn purge(&mut self, id: u64) {}
}
