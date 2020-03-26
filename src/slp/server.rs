use tokio::io::Result;
use tokio::net::{UdpSocket, udp::RecvHalf};
use tokio::sync::{RwLock, mpsc, broadcast};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use lru::LruCache;
use tokio::time::{Instant, timeout_at};
use serde::Serialize;
use juniper::{GraphQLObject, FieldError};
use futures::{stream::{StreamExt, BoxStream}, future};
use crate::graphql::FilterSameExt;

/// Infomation about this server
#[derive(Clone, Debug, Eq, PartialEq, Serialize, GraphQLObject)]
pub struct ServerInfo {
    /// The number of online clients
    online: i32,
    /// The version of the server
    version: String,
}

struct PeerInner {
    rx: mpsc::Receiver<Vec<u8>>,
    addr: SocketAddr,
    event_send: mpsc::Sender<Event>,
}
struct Peer {
    sender: mpsc::Sender<Vec<u8>>,
}
impl Peer {
    fn new(addr: SocketAddr, event_send: mpsc::Sender<Event>) -> Self {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(10);
        tokio::spawn(async move {
            Self::do_packet(PeerInner {
                rx,
                addr,
                event_send,
            }).await;
        });
        Self {
            sender: tx,
        }
    }
    async fn on_packet(&self, data: Vec<u8>) {
        self.sender.clone().send(data).await.unwrap()
    }
    async fn do_packet(inner: PeerInner) {
        let PeerInner { mut rx, addr, mut event_send } = inner;
        loop {
            let deadline = Instant::now() + Duration::from_secs(30);
            let packet = match timeout_at(deadline, rx.recv()).await {
                Ok(Some(packet)) => packet,
                _ => {
                    log::info!("Timeout");
                    break
                },
            };
            log::info!("on_packet: {:?} {}", packet, addr);
        }
        event_send.send(Event::Close(addr)).await.unwrap();
    }
}

#[derive(Debug)]
enum Event {
    Close(SocketAddr),
    Unicast(SocketAddr, Vec<u8>),
    Broadcast(SocketAddr, Vec<u8>),
}

struct InnerServer {
    cache: LruCache<SocketAddr, Peer>,
}
impl InnerServer {
    fn new() -> Self {
        Self {
            cache: LruCache::new(100),
        }
    }
    fn server_info(&self) -> ServerInfo {
        ServerInfo {
            online: self.cache.len() as i32,
            version: std::env!("CARGO_PKG_VERSION").to_owned(),
        }
    }
}

#[derive(Clone)]
pub struct UDPServer {
    inner: Arc<RwLock<InnerServer>>,
    info_stream: broadcast::Sender<ServerInfo>,
}

type Packet = (SocketAddr, Vec<u8>);
impl UDPServer {
    pub async fn new(addr: &str) -> Result<Self> {
        let inner = Arc::new(RwLock::new(InnerServer::new()));
        let inner2 = inner.clone();
        let inner3 = inner.clone();
        let inner4 = inner.clone();
        let (tx, rx) = mpsc::channel::<Packet>(10);
        let (event_send, mut event_recv) = mpsc::channel::<Event>(1);
        let (recv_half, mut send_half) = UdpSocket::bind(addr).await?.split();

        tokio::spawn(async {
            if let Err(err) = Self::recv(recv_half, tx).await {
                log::error!("recv thread exited. reason: {:?}", err);
            }
        });
        tokio::spawn(async move {
            if let Err(err) = Self::on_packet(rx, inner2, event_send).await {
                log::error!("on_packet thread exited. reason: {:?}", err);
            }
        });
        tokio::spawn(async move {
            let inner = inner3;
            while let Some(event) = event_recv.recv().await {
                match event {
                    Event::Close(addr) => {
                        inner.write().await.cache.pop(&addr);
                    },
                    Event::Unicast(addr, packet) => {
                        send_half.send_to(&packet, &addr).await.unwrap();
                    },
                    Event::Broadcast(exp_addr, packet) => {
                        for (addr, _) in inner.write().await.cache.iter() {
                            if &exp_addr == addr {
                                continue;
                            }
                            send_half.send_to(&packet, &addr).await.unwrap();
                        }
                    }
                }
            }
        });

        let (info_stream, rx) = broadcast::channel(10);
        let info_sender = info_stream.clone();
        tokio::spawn(async move {
            rx;
            tokio::time::interval(
                Duration::from_secs(1)
            )
            .then(move |_| {
                let inner = inner4.clone();
                async move {
                    let inner = inner.read().await;
                    inner.server_info()
                }
            })
            .filter_same()
            .map(|info| {
                let info_sender = info_sender.clone();
                info_sender.send(info.clone()).is_ok()
            })
            .take_while(|ok| future::ready(*ok))
            .for_each(|_| future::ready(()))
            .await;
        });

        Ok(Self {
            inner,
            info_stream,
        })
    }
    async fn on_packet(mut receiver: mpsc::Receiver<Packet>, inner: Arc<RwLock<InnerServer>>, event_send: mpsc::Sender<Event>) -> Result<()> {
        loop {
            let (addr, buffer) = receiver.recv().await.unwrap();
            let cache = &mut inner.write().await.cache;
            let peer = {
                if cache.peek(&addr).is_none() {
                    cache.put(addr, Peer::new(addr, event_send.clone()));
                }
                cache.get_mut(&addr).unwrap()
            };
            peer.on_packet(buffer).await;
        }
    }
    async fn recv(mut recv: RecvHalf, mut sender: mpsc::Sender<Packet>) -> Result<()> {
        loop {
            let mut buffer = vec![0u8; 65536];
            let (size, addr) = recv.recv_from(&mut buffer).await?;
            buffer.truncate(size);

            sender.send((addr, buffer)).await.unwrap();
        }
    }
    pub async fn server_info(&self) -> ServerInfo {
        let inner = self.inner.read().await;

        inner.server_info()
    }
    pub fn server_info_stream(&self) -> BoxStream<'static, std::result::Result<ServerInfo, FieldError>> {
        self.info_stream.subscribe()
        .map(|info| {
            info.map_err(|err| FieldError::new(
                "Failed to recv",
                juniper::Value::null()
            ))
        })
        .boxed()
    }
}
