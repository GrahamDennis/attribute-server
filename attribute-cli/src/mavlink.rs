use crate::attributes::TypedAttribute;
use crate::pb::mavlink::{GlobalPosition, MissionCurrent};
use crate::pb::{
    AttributeType, AttributeValue, CreateAttributeTypeRequest, EntityLocator, UpdateEntityRequest,
    ValueType,
};
use crate::{pb, Cli};
use clap::Args;
use maviola::asnc::node::Event;
use maviola::asnc::prelude::{EdgeNode, ReceiveEvent, StreamExt};
use maviola::dialects::Ardupilotmega;
use maviola::prelude::default_dialect::messages;
use maviola::prelude::{CallbackApi, DefaultDialect, Frame, Network, Node, TcpClient, TcpServer};
use maviola::protocol::{ComponentId, MavLinkId, SystemId, V2};
use prost::Message;
use std::convert::Into;
use std::string::ToString;
use std::sync::LazyLock;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tonic::Code;
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

pub enum AttributeTypes {
    GlobalPosition,
    MissionCurrent,
    FileDescriptorSet,
    FileDescriptorSetRef,
    MessageName,
}

impl TypedAttribute for GlobalPosition {
    fn attribute_name() -> &'static str {
        "mavlink/globalPosition"
    }

    fn as_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}

impl TypedAttribute for MissionCurrent {
    fn attribute_name() -> &'static str {
        "mavlink/missionCurrent"
    }

    fn as_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}

impl AttributeTypes {
    pub fn as_str(&self) -> &'static str {
        match self {
            AttributeTypes::GlobalPosition => "mavlink/globalPosition",
            AttributeTypes::MissionCurrent => "mavlink/missionCurrent",
            AttributeTypes::FileDescriptorSet => "pb/fileDescriptorSet",
            AttributeTypes::FileDescriptorSetRef => "pb/fileDescriptorSetRef",
            AttributeTypes::MessageName => "pb/messageName",
        }
    }
}

enum EntityNames {
    MavlinkFileDescriptorSet,
}

impl EntityNames {
    fn as_str(&self) -> &'static str {
        match self {
            EntityNames::MavlinkFileDescriptorSet => "mavlink/fileDescriptorSet",
        }
    }
}

static ATTRIBUTE_TYPES: LazyLock<Vec<CreateAttributeTypeRequest>> = LazyLock::new(|| {
    vec![
        CreateAttributeTypeRequest {
            attribute_type: Some(AttributeType {
                symbol: AttributeTypes::FileDescriptorSet.as_str().to_string(),
                value_type: ValueType::Bytes.into(),
            }),
        },
        CreateAttributeTypeRequest {
            attribute_type: Some(AttributeType {
                symbol: AttributeTypes::FileDescriptorSetRef.as_str().to_string(),
                value_type: ValueType::EntityReference.into(),
            }),
        },
        CreateAttributeTypeRequest {
            attribute_type: Some(AttributeType {
                symbol: AttributeTypes::MessageName.as_str().to_string(),
                value_type: ValueType::Text.into(),
            }),
        },
        CreateAttributeTypeRequest {
            attribute_type: Some(AttributeType {
                symbol: AttributeTypes::GlobalPosition.as_str().to_string(),
                value_type: ValueType::Bytes.into(),
            }),
        },
        CreateAttributeTypeRequest {
            attribute_type: Some(AttributeType {
                symbol: AttributeTypes::MissionCurrent.as_str().to_string(),
                value_type: ValueType::Bytes.into(),
            }),
        },
    ]
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

impl From<messages::MissionCurrent> for pb::mavlink::MissionCurrent {
    fn from(value: messages::MissionCurrent) -> Self {
        MissionCurrent {
            sequence: value.seq as u32,
            total_mission_items: value.total as u32,
            mission_state: value.mission_state as i32,
            mission_mode: value.mission_mode as i32,
            mission_id: value.mission_id,
            fence_id: value.fence_id,
            rally_points_id: value.rally_points_id,
        }
    }
}

struct MavlinkProcessor {
    // FIXME: This isn't scalable; we need to support subscribing to streams of arbitrary message types
    // or a one-shot response with a timeout
    global_position_tx: Sender<(Frame<V2>, messages::GlobalPositionInt)>,
    mission_current_tx: Sender<(Frame<V2>, messages::MissionCurrent)>,
}

impl MavlinkProcessor {
    fn new() -> Self {
        let (global_position_tx, _) = broadcast::channel(16);
        let (mission_current_tx, _) = broadcast::channel(16);
        MavlinkProcessor {
            global_position_tx,
            mission_current_tx,
        }
    }

    async fn process_events(&self, node: &EdgeNode<V2>) -> anyhow::Result<()> {
        let mut events = node.events().unwrap();
        while let Some(event) = events.next().await {
            self.process_event(event)?;
        }

        Ok(())
    }

    fn process_event(&self, event: Event<V2>) -> anyhow::Result<()> {
        match event {
            Event::NewPeer(peer) => {
                println!("New MAVLink device joined the network: {:?}", peer);
            }
            Event::PeerLost(peer) => {
                println!("MAVLink device is no longer active: {:?}", peer);
            }
            Event::Frame(frame, callback) => {
                callback.broadcast(&frame).unwrap();
                if let Ok(message) = frame.decode::<DefaultDialect>() {
                    log::debug!(
                        "Received a message from {}:{}: {:?}",
                        frame.system_id(),
                        frame.component_id(),
                        message
                    );

                    match message {
                        Ardupilotmega::GlobalPositionInt(global_position_int) => {
                            self.global_position_tx.send((frame, global_position_int))?;
                        }
                        Ardupilotmega::MissionCurrent(mission_current) => {
                            self.mission_current_tx.send((frame, mission_current))?;
                        }
                        _ => {}
                    }
                }
            }
            Event::Invalid(..) => {}
        }
        Ok(())
    }
}

fn symbol_for_node(frame: &Frame<V2>) -> String {
    format!("mavlink/id/{}:{}", frame.system_id(), frame.component_id())
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

    log::info!("Creating entities");

    {
        let create_mavlink_fdset_request = UpdateEntityRequest {
            entity_locator: Some(EntityLocator::from_symbol(
                EntityNames::MavlinkFileDescriptorSet.as_str(),
            )),
            attributes_to_update: vec![
                pb::AttributeToUpdate {
                    attribute_type: "@symbolName".to_string(),
                    attribute_value: Some(AttributeValue::from_string(
                        EntityNames::MavlinkFileDescriptorSet.as_str(),
                    )),
                },
                pb::AttributeToUpdate {
                    attribute_type: AttributeTypes::FileDescriptorSet.as_str().to_string(),
                    attribute_value: Some(AttributeValue::from_bytes(
                        pb::mavlink::FILE_DESCRIPTOR_SET.to_vec(),
                    )),
                },
            ],
        };
        let mavlink_fdset_response = attribute_store_client
            .update_entity(create_mavlink_fdset_request)
            .await?
            .into_inner();
        let mavlink_fdset_entity = mavlink_fdset_response
            .entity
            .ok_or(anyhow::format_err!("Failed to create mavlink fdset entity"))?;
        let mavlink_fdset_entity_id = mavlink_fdset_entity.entity_id;

        attribute_store_client
            .update_protobuf_attribute_type::<GlobalPosition>(&mavlink_fdset_entity_id)
            .await?;
        attribute_store_client
            .update_protobuf_attribute_type::<MissionCurrent>(&mavlink_fdset_entity_id)
            .await?;
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
        .await?;

    // Activate node to start sending heartbeats
    node.activate().await?;

    let mavlink_processor = MavlinkProcessor::new();
    let mut global_position_rx = mavlink_processor.global_position_tx.subscribe();
    let mut mission_current_rx = mavlink_processor.mission_current_tx.subscribe();

    let join_handle = tokio::spawn(async move { mavlink_processor.process_events(&node).await });

    loop {
        tokio::select! {
            Ok((frame, global_position_int)) = global_position_rx.recv() => {
            let global_position: pb::mavlink::GlobalPosition = global_position_int.into();
                let symbol_id = symbol_for_node(&frame);
                let response = attribute_store_client.simple_update_entity(&symbol_id, global_position).await?;
            }
            Ok((frame, mission_current)) = mission_current_rx.recv() => {
                let mission_current_proto: pb::mavlink::MissionCurrent = mission_current.into();
                let symbol_id = symbol_for_node(&frame);

                let response = attribute_store_client.simple_update_entity(&symbol_id, mission_current_proto).await?;
            }
                else => {
                    break;
                }
        }
    }

    join_handle.abort();

    Ok(())
}

// async fn fetch_mission() -> anyhow::Result<()> {
//
// }
