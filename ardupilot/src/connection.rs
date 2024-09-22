use crate::codec::MavlinkCodec;
use futures::SinkExt;
use mavio::prelude::MaybeVersioned;
use mavio::protocol::{ComponentId, Sequencer, SystemId, Versioned};
use mavio::{Dialect, Frame, Message};
use mavspec_rust_spec::MessageSpecStatic;
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
    Local,
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
            let frame = routable_frame.frame;
            let header = frame.header();
            if let Ok(message) = frame.decode::<D>() {
                tracing::debug!(system_id=header.system_id(), component_id=header.component_id(), ?message, origin=?routable_frame.origin, "Received message");
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

pub struct MavlinkClient<V: Versioned> {
    mavlink_network: MavlinkNetwork<V>,
    pub system_id: SystemId,
    pub component_id: ComponentId,
    sequencer: Sequencer,
}

impl<V: Versioned> MavlinkClient<V> {
    pub fn create(
        mavlink_network: MavlinkNetwork<V>,
        system_id: SystemId,
        component_id: ComponentId,
    ) -> MavlinkClient<V> {
        MavlinkClient {
            mavlink_network,
            system_id,
            component_id,
            sequencer: Sequencer::new(),
        }
    }

    #[inline(always)]
    pub async fn send_and_await_response<
        RequestT: Message + std::fmt::Debug,
        ResponseT: MessageSpecStatic + for<'a> TryFrom<&'a mavspec_rust_spec::Payload> + std::fmt::Debug,
    >(
        &mut self,
        msg: RequestT,
    ) -> anyhow::Result<ResponseT> {
        self.send_and_await_response_with_predicate(msg, |_| true)
            .await
    }

    pub async fn send_and_await_response_with_predicate<
        RequestT: Message + std::fmt::Debug,
        ResponseT: MessageSpecStatic + for<'a> TryFrom<&'a mavspec_rust_spec::Payload> + std::fmt::Debug,
        ResponsePredicate: Fn(&ResponseT) -> bool,
    >(
        &mut self,
        request: RequestT,
        response_predicate: ResponsePredicate,
    ) -> anyhow::Result<ResponseT> {
        let tx = &mut self.mavlink_network.tx;
        let mut rx = tx.subscribe();
        let frame = Frame::builder()
            .version(V::v())
            .message(&request)?
            .sequence(self.sequencer.next())
            .system_id(self.system_id)
            .component_id(self.component_id)
            .build();

        tracing::info!(?request, "Sending request");
        tx.send(RoutableFrame {
            frame,
            origin: ConnectionId::Local,
            destination: MavlinkDestination::All,
        })?;

        // FIXME: add timeout
        loop {
            let routable_frame = rx.recv().await?;
            if routable_frame.frame.message_id() != ResponseT::message_id() {
                continue;
            }
            if let Ok(response_candidate) = ResponseT::try_from(routable_frame.frame.payload()) {
                if !response_predicate(&response_candidate) {
                    continue;
                }
                tracing::info!(?request, response=?response_candidate, "Received response");
                return Ok(response_candidate);
            };
        }
    }
}
