use crate::codec::MavlinkCodec;
use futures::SinkExt;
use mavio::prelude::MaybeVersioned;
use mavio::{Dialect, Frame};
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::log;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ConnectionId {
    Tcp {
        local_addr: SocketAddr,
        peer_addr: SocketAddr,
    },
}

impl ConnectionId {
    fn create(stream: &TcpStream) -> anyhow::Result<ConnectionId> {
        Ok(ConnectionId::Tcp {
            local_addr: stream.local_addr()?,
            peer_addr: stream.peer_addr()?,
        })
    }
}

#[derive(Copy, Clone, Debug)]
enum MavlinkDestination {
    All,
    NotConnectionId(ConnectionId),
    OnlyConnectionId(ConnectionId),
}

#[derive(Clone, Debug)]
pub struct RoutableFrame<V: MaybeVersioned> {
    frame: Frame<V>,
    origin: ConnectionId,
    destination: MavlinkDestination,
}

#[derive(Clone, Debug)]
pub struct MavlinkNetwork<V: MaybeVersioned> {
    tx: Sender<RoutableFrame<V>>,
}

impl<V: MaybeVersioned> MavlinkNetwork<V> {
    #[inline(always)]
    pub fn create_with_capacity(capacity: usize) -> MavlinkNetwork<V> {
        Self::create(Sender::new(capacity))
    }

    #[inline(always)]
    pub fn create(tx: Sender<RoutableFrame<V>>) -> MavlinkNetwork<V> {
        MavlinkNetwork { tx }
    }

    pub async fn accept_loop(self, listener: TcpListener) -> anyhow::Result<()> {
        loop {
            let (socket, peer_addr) = listener.accept().await?;
            tracing::info!(%peer_addr, "Received connection");
            // A new task is spawned for each inbound socket. The socket is
            // moved to the new task and processed there.
            tokio::spawn(self.clone().process_tcp(socket));
        }
    }

    pub async fn process_tcp(self, mut tcp_stream: TcpStream) -> anyhow::Result<()> {
        let connection_id = ConnectionId::create(&tcp_stream)?;
        let (read, write) = tcp_stream.split();

        self.process(connection_id, read, write).await
    }

    pub async fn log_frames<D: Dialect + std::fmt::Debug>(self) -> anyhow::Result<()> {
        let mut rx = self.tx.subscribe();
        loop {
            let routable_frame = rx.recv().await?;
            if let Ok(message) = routable_frame.frame.decode::<D>() {
                tracing::debug!(?message, origin=?routable_frame.origin, "Received message");
            }
        }
    }

    #[tracing::instrument(skip(self, read, write))]
    async fn process<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        self,
        connection_id: ConnectionId,
        read: R,
        write: W,
    ) -> anyhow::Result<()> {
        tracing::info!("Processing connection");

        let mut framed_reader = FramedRead::new(read, MavlinkCodec::<V>::new());
        let mut framed_writer = FramedWrite::new(write, MavlinkCodec::<V>::new());

        let mut channel_rx = self.tx.subscribe();

        loop {
            tokio::select! {
                socket_result = framed_reader.next() => {
                    let Some(frame_result) = socket_result else {
                        log::info!("Disconnected");
                        return Ok(());
                    };
                    let frame = frame_result?;

                    let routable_frame = RoutableFrame {
                        frame, origin: connection_id, destination: MavlinkDestination::NotConnectionId(connection_id)
                    };

                    self.tx.send(routable_frame)?;
                }
                channel_result = channel_rx.recv() => {
                    let Ok(routable_frame) = channel_result else {
                        return Ok(())
                    };
                    match routable_frame.destination {
                        MavlinkDestination::All => {},
                        MavlinkDestination::NotConnectionId(not_connection_id) => {
                            if not_connection_id == connection_id {
                                continue;
                            }
                        },
                        MavlinkDestination::OnlyConnectionId(only_connection_id) => {
                            if only_connection_id != connection_id {
                                continue;
                            }
                        }}

                    framed_writer.send(routable_frame.frame).await?;
                }
            }
        }
    }
}
