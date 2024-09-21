use mavio::protocol::V2;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::FramedRead;
use tracing::level_filters::LevelFilter;
use tracing::log;
use tracing_subscriber::EnvFilter;

mod codec;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let socket = TcpStream::connect("127.0.0.1:5760").await?;

    let codec = codec::MavlinkCodec::<V2>::new();

    let mut reader = FramedRead::new(socket, codec);

    loop {
        let frame = reader.next().await.unwrap()?;
        if let Ok(message) = frame.decode::<mavio::dialects::Ardupilotmega>() {
            log::debug!(
                "Received a message from {}:{}: {:?}",
                frame.system_id(),
                frame.component_id(),
                message
            );
        }
    }

    Ok(())
}
