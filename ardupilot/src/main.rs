use ardupilot::connection::{MavlinkClient, MavlinkNetwork};
use ardupilot::mission::MissionProtocol;
use mavio::dialects::common::enums::MavMissionType;
use mavio::dialects::common::messages::{MissionRequestInt, MissionRequestList};
use mavio::dialects::Ardupilotmega;
use mavio::protocol::{ComponentId, SystemId, Versioned, V2};
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

    let mut mavlink_network = MavlinkNetwork::<V2>::create_with_capacity(128);
    let mut join_set = JoinSet::new();
    let socket = TcpStream::connect("127.0.0.1:5760").await?;
    join_set.spawn(mavlink_network.clone().process_tcp(socket));

    let listener = TcpListener::bind("127.0.0.1:5600").await?;

    join_set.spawn(mavlink_network.clone().accept_loop(listener));
    join_set.spawn(mavlink_network.clone().log_frames::<Ardupilotmega>());

    sleep(Duration::from_secs(1)).await;
    let mut mavlink_client = MavlinkClient::create(mavlink_network.clone(), 99, 99);
    let _ = fetch_mission(&mut mavlink_client).await;

    join_set.join_all().await;

    Ok(())
}

async fn fetch_mission<V: Versioned>(mavlink_client: &mut MavlinkClient<V>) -> anyhow::Result<()> {
    let mission_type = MavMissionType::Mission;
    let target_system: SystemId = 1;
    let target_component: ComponentId = 1;
    let mission_count = mavlink_client
        .request_list(MissionRequestList {
            target_system,
            target_component,
            mission_type,
        })
        .await?;

    for idx in 0..mission_count.count {
        let _mission_item = mavlink_client
            .request_int(MissionRequestInt {
                target_system,
                target_component,
                seq: idx,
                mission_type,
            })
            .await?;
    }

    Ok(())
}
