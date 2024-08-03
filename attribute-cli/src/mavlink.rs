use crate::pb::{
    AttributeType, AttributeValue, CreateAttributeTypeRequest, CreateAttributeTypeResponse,
    ValueType,
};
use crate::{pb, Cli};
use clap::Args;
use maviola::asnc::node::Event;
use maviola::asnc::prelude::{EdgeNode, ReceiveEvent, StreamExt};
use maviola::dialects::Ardupilotmega;
use maviola::prelude::default_dialect::messages;
use maviola::prelude::{
    default_dialect, CallbackApi, DefaultDialect, Frame, Network, Node, TcpClient, TcpServer,
};
use maviola::protocol::{ComponentId, MavLinkId, SystemId, V2};
use prost::Message;
use std::convert::Into;
use std::sync::LazyLock;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tonic::codegen::tokio_stream::Stream;
use tonic::{Code, Response, Status};
use tracing::log;

#[derive(Args)]
pub struct MavlinkArgs {
    #[arg(long = "server-endpoint")]
    server_endpoints: Vec<String>,
    #[arg(long = "client-endpoint")]
    client_endpoints: Vec<String>,
    #[arg(long, default_value_t = 99)]
    system_id: SystemId,
    #[arg(long, default_value_t = 17)]
    component_id: ComponentId,
}

enum AttributeTypes {
    GlobalPosition,
}

impl AttributeTypes {
    fn as_str(&self) -> &'static str {
        match self {
            AttributeTypes::GlobalPosition => "mavlink/globalPosition",
        }
    }
}

static ATTRIBUTE_TYPES: LazyLock<Vec<CreateAttributeTypeRequest>> = LazyLock::new(|| {
    vec![CreateAttributeTypeRequest {
        attribute_type: Some(AttributeType {
            symbol: AttributeTypes::GlobalPosition.as_str().to_string(),
            value_type: ValueType::Bytes.into(),
        }),
    }]
});

fn from_mavlink_deg_e7(degrees: i32) -> f64 {
    f64::from(degrees) / 1e7
}

fn from_mavlink_vertical_position_mm(vertical_position: i32) -> f64 {
    f64::from(vertical_position) / 1e3
}

fn from_mavlink_velocity_cm_s(velocity: i16) -> f32 {
    f32::from(velocity) / 1e2
}

fn from_mavlink_orientation_cdeg(orientation: u16) -> f32 {
    f32::from(orientation) / 1e2
}

impl From<messages::GlobalPositionInt> for pb::mavlink::GlobalPosition {
    fn from(value: messages::GlobalPositionInt) -> Self {
        pb::mavlink::GlobalPosition {
            time_boot_ms: value.time_boot_ms,
            latitude_deg: from_mavlink_deg_e7(value.lat),
            longitude_deg: from_mavlink_deg_e7(value.lon),
            alt_msl_m: from_mavlink_vertical_position_mm(value.alt),
            relative_alt_agl_m: from_mavlink_vertical_position_mm(value.relative_alt),
            velocity_x_m_s: from_mavlink_velocity_cm_s(value.vx),
            velocity_y_m_s: from_mavlink_velocity_cm_s(value.vy),
            velocity_z_m_s: from_mavlink_velocity_cm_s(value.vz),
            heading_deg: from_mavlink_orientation_cdeg(value.hdg),
        }
    }
}

struct MavlinkProcessor {
    global_position_channel: Sender<(Frame<V2>, messages::GlobalPositionInt)>,
}

impl MavlinkProcessor {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(16);
        MavlinkProcessor {
            global_position_channel: tx,
        }
    }

    async fn process_events(&self, node: &EdgeNode<V2>) -> anyhow::Result<()> {
        let mut events = node.events().unwrap();
        while let Some(event) = events.next().await {
            match event {
                Event::NewPeer(peer) => {
                    println!("New MAVLink device joined the network: {:?}", peer);
                }
                Event::PeerLost(peer) => {
                    println!("MAVLink device is no longer active: {:?}", peer);
                }
                Event::Frame(frame, callback) => {
                    if let Ok(message) = frame.decode::<DefaultDialect>() {
                        log::debug!(
                            "Received a message from {}:{}: {:?}",
                            frame.system_id(),
                            frame.component_id(),
                            message
                        );

                        callback.broadcast(&frame).unwrap();

                        match message {
                            Ardupilotmega::GlobalPositionInt(global_position_int) => {
                                self.global_position_channel
                                    .send((frame, global_position_int))?;
                            }
                            _ => {}
                        }
                    }
                }
                Event::Invalid(..) => {}
            }
        }

        Ok(())
    }
}

pub async fn mavlink_run(cli: &Cli, args: &MavlinkArgs) -> anyhow::Result<()> {
    let mut attribute_store_client = crate::create_attribute_store_client(&cli.endpoint).await?;

    log::info!("Creating attribute types");

    for create_attribute_type_request in ATTRIBUTE_TYPES.iter() {
        let result = attribute_store_client
            .create_attribute_type(create_attribute_type_request.clone())
            .await;
        match result {
            Ok(_) => {}
            Err(status) if status.code() == Code::AlreadyExists => {
                log::debug!("skipping attribute because it already exists");
            }
            Err(status) => {
                return Err(status)?;
            }
        }
    }

    println!("Mavlink running...");

    println!("Server endpoints: {:?}", args.server_endpoints);
    println!("Client endpoints: {:?}", args.client_endpoints);

    let mut network = Network::asnc();

    for server_address in &args.server_endpoints {
        network = network.add_connection(TcpServer::new(server_address)?);
    }
    for client_address in &args.client_endpoints {
        network = network.add_connection(TcpClient::new(client_address)?);
    }

    let mut node = Node::asnc::<V2>()
        .id(MavLinkId::new(args.system_id, args.component_id))
        .connection(network)
        .build()
        .await
        .unwrap();

    // Activate node to start sending heartbeats
    node.activate().await.unwrap();

    let mavlink_processor = MavlinkProcessor::new();
    let mut global_position_rx = mavlink_processor.global_position_channel.subscribe();

    let join_handle = tokio::spawn(async move { mavlink_processor.process_events(&node).await });

    loop {
        tokio::select! {
            Ok((frame, global_position_int)) = global_position_rx.recv() => {
            let global_position: pb::mavlink::GlobalPosition = global_position_int.into();
                // FIXME: push onto queue for publishing to support discarding
                let symbol_id = format!("mavlink/id/{}:{}", frame.system_id(), frame.component_id());
                let response = attribute_store_client.update_entity(pb::UpdateEntityRequest {
                    entity_locator: Some(pb::EntityLocator { locator: Some(pb::entity_locator::Locator::Symbol(
                        symbol_id.clone()
                    )) }),
                    attributes_to_update: vec![
                        pb::AttributeToUpdate {
                            attribute_type: "@symbolName".to_string(),
                            attribute_value: Some(AttributeValue { attribute_value: Some(pb::attribute_value::AttributeValue::StringValue(
                                symbol_id.clone()
                            )) }), },
                        pb::AttributeToUpdate {
                            attribute_type: AttributeTypes::GlobalPosition.as_str().to_string(),
                            attribute_value: Some(AttributeValue { attribute_value: Some(pb::attribute_value::AttributeValue::BytesValue(
                                global_position.encode_to_vec()
                            )) }),
                        }
                    ],
                }).await?;
            }
                else => {
                    break;
                }
        }
    }

    join_handle.abort();

    Ok(())
}
