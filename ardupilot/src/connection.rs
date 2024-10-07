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
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
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
pub struct Network<V: MaybeVersioned> {
    tx: Sender<RoutableFrame<V>>,
}

impl<V: MaybeVersioned> Network<V> {
    #[inline(always)]
    pub fn create_with_capacity(capacity: usize) -> Network<V> {
        Self::create(Sender::new(capacity))
    }

    #[inline(always)]
    pub fn create(tx: Sender<RoutableFrame<V>>) -> Network<V> {
        Network { tx }
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

    pub async fn subscribe<
        MessageT: MessageSpecStatic + for<'a> TryFrom<&'a mavspec_rust_spec::Payload>,
    >(
        &self,
    ) -> impl Stream<Item = (NodeId, MessageT)> {
        let rx = self.tx.subscribe();
        BroadcastStream::new(rx).filter_map(move |frame_result| {
            let routable_frame = frame_result.ok()?;
            let frame = routable_frame.frame;
            let origin_node_id = NodeId {
                system_id: frame.system_id(),
                component_id: frame.component_id(),
            };
            if frame.message_id() != MessageT::message_id() {
                return None;
            }

            if let Ok(message) = MessageT::try_from(frame.payload()) {
                return Some((origin_node_id, message));
            }

            None
        })
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId {
    pub system_id: SystemId,
    pub component_id: ComponentId,
}

pub type MessageFromNode<M> = (NodeId, M);

pub struct Client<V: Versioned> {
    network: Network<V>,
    pub node_id: NodeId,
    sequencer: Sequencer,
}

impl<V: Versioned> Client<V> {
    pub fn create(mavlink_network: Network<V>, node_id: NodeId) -> Client<V> {
        Client {
            network: mavlink_network,
            node_id,
            sequencer: Sequencer::new(),
        }
    }

    pub fn response_type_message_extractor<
        ResponseT: MessageSpecStatic + for<'a> TryFrom<&'a mavspec_rust_spec::Payload> + std::fmt::Debug,
    >() -> impl Fn(&Frame<V>) -> Option<ResponseT> {
        |frame| {
            if frame.message_id() != ResponseT::message_id() {
                return None;
            }
            ResponseT::try_from(frame.payload()).ok()
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
        self.send_and_await_response_with_extractor(msg, Self::response_type_message_extractor())
            .await
    }

    pub async fn send_and_await_response_with_extractor<
        RequestT: Message + std::fmt::Debug,
        ResponseT: std::fmt::Debug,
        ResponseExtractor: Fn(&Frame<V>) -> Option<ResponseT>,
    >(
        &mut self,
        request: RequestT,
        response_extractor: ResponseExtractor,
    ) -> anyhow::Result<ResponseT> {
        let tx = &mut self.network.tx;
        let mut rx = tx.subscribe();
        let frame = Frame::builder()
            .version(V::v())
            .message(&request)?
            .sequence(self.sequencer.next())
            .system_id(self.node_id.system_id)
            .component_id(self.node_id.component_id)
            .build();

        tracing::debug!(?request, "Sending request");
        tx.send(RoutableFrame {
            frame,
            origin: ConnectionId::Local,
            destination: MavlinkDestination::All,
        })?;

        // FIXME: add timeout
        loop {
            let routable_frame = rx.recv().await?;
            if let Some(response) = response_extractor(&routable_frame.frame) {
                tracing::debug!(?request, response=?response, "Received response");
                return Ok(response);
            }
        }
    }
}
