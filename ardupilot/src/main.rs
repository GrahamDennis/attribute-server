use ardupilot::connection::MavlinkNetwork;
use mavio::dialects::Ardupilotmega;
use mavio::protocol::V2;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let mavlink_network = MavlinkNetwork::<V2>::create_with_capacity(128);
    let mut join_set = JoinSet::new();
    let socket = TcpStream::connect("127.0.0.1:5760").await?;
    join_set.spawn(mavlink_network.clone().process_tcp(socket));

    let listener = TcpListener::bind("127.0.0.1:5600").await?;

    join_set.spawn(mavlink_network.clone().accept_loop(listener));
    join_set.spawn(mavlink_network.log_frames::<Ardupilotmega>());

    join_set.join_all().await;

    Ok(())
}
