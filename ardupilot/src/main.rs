use futures::sink::SinkExt;
use mavio::dialects::Ardupilotmega;
use mavio::protocol::V2;
use mavio::Frame;
use std::net::SocketAddr;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::level_filters::LevelFilter;
use tracing::log;
use tracing_subscriber::EnvFilter;

mod codec;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct ConnectionAddr {
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
}

impl ConnectionAddr {
    fn get(stream: &TcpStream) -> Result<ConnectionAddr, io::Error> {
        Ok(ConnectionAddr {
            local_addr: stream.local_addr()?,
            peer_addr: stream.peer_addr()?,
        })
    }
}

type SourcedFrame<V> = (Frame<V>, ConnectionAddr);

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let listener = TcpListener::bind("127.0.0.1:5600").await?;
    let (tx, _) = broadcast::channel::<SourcedFrame<V2>>(128);

    let handle1 = tokio::spawn(accept(listener, tx.clone()));

    let socket = TcpStream::connect("127.0.0.1:5760").await?;
    let handle2 = tokio::spawn(process(socket, tx));

    let (result1, result2) = tokio::join!(handle1, handle2);
    let _ = result1??;
    let _ = result2??;

    Ok(())
}

async fn accept(listener: TcpListener, tx: Sender<SourcedFrame<V2>>) -> Result<(), io::Error> {
    loop {
        let (socket, socket_addr) = listener.accept().await?;
        let tx_clone = tx.clone();
        // A new task is spawned for each inbound socket. The socket is
        // moved to the new task and processed there.
        tokio::spawn(async move { process(socket, tx_clone).await });
    }
}

async fn process(
    mut socket: TcpStream,
    channel_tx: Sender<SourcedFrame<V2>>,
) -> Result<(), io::Error> {
    let connection_addr = ConnectionAddr::get(&socket)?;
    let codec = codec::MavlinkCodec::<V2>::new();
    let (reader, writer) = socket.split();
    let mut framed_reader = FramedRead::new(reader, codec);
    let mut framed_writer = FramedWrite::new(writer, codec);

    let mut channel_rx = channel_tx.subscribe();

    loop {
        tokio::select! {
            socket_result = framed_reader.next() => {
                let frame = socket_result.unwrap()?;
                if let Ok(message) = frame.decode::<Ardupilotmega>() {
                    log::debug!(
                        "Received a message from {}:{}: {:?}",
                        frame.system_id(),
                        frame.component_id(),
                        message
                    );
                }

                channel_tx.send((frame, connection_addr)).unwrap();
            }
            channel_result = channel_rx.recv() => {
                let (frame, rx_connection_addr) = channel_result.unwrap();
                if rx_connection_addr == connection_addr {
                    continue;
                }

                framed_writer.send(frame).await?;
            }
        }
    }
}
