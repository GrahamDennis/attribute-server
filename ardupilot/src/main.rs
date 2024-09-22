use ardupilot::connection::{Client, Network, NodeId};
use ardupilot::mission::MissionProtocol;
use mavio::dialects::Ardupilotmega;
use mavio::protocol::V2;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::sleep;
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

    let mavlink_network = Network::<V2>::create_with_capacity(128);
    let mut join_set = JoinSet::new();
    let socket = TcpStream::connect("127.0.0.1:5760").await?;
    join_set.spawn(mavlink_network.clone().process_tcp(socket));

    let listener = TcpListener::bind("127.0.0.1:5600").await?;

    join_set.spawn(mavlink_network.clone().accept_loop(listener));
    join_set.spawn(mavlink_network.clone().log_frames::<Ardupilotmega>());

    sleep(Duration::from_secs(1)).await;
    let mut mavlink_client = Client::create(
        mavlink_network.clone(),
        NodeId {
            system_id: 99,
            component_id: 99,
        },
    );
    join_set.spawn(async move {
        loop {
            let mission = mavlink_client
                .fetch_mission(NodeId {
                    system_id: 1,
                    component_id: 1,
                })
                .await;
            tracing::info!(?mission, "Current mission");
            sleep(Duration::from_secs(5)).await;
        }
    });

    join_set.join_all().await;

    Ok(())
}
