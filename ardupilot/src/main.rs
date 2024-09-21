use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

mod lib;

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

    Ok(())
}
