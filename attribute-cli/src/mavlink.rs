use crate::attributes::TypedAttribute;
use crate::pb::attribute_store_client::AttributeStoreClient;
use crate::pb::mavlink::{GlobalPosition, Mission, MissionCurrent, MissionItem};
use crate::pb::{
    AttributeType, AttributeValue, CreateAttributeTypeRequest, EntityLocator,
    UpdateEntityRequest, ValueType,
};
use crate::{pb, Cli};
use anyhow::format_err;
use ardupilot::connection::{Client, Network, NodeId};
use ardupilot::mission::MissionProtocol;
use clap::Args;
use mavio::dialects::common::messages;
use mavio::dialects::common::messages::MissionItemInt;
use mavio::protocol::{ComponentId, SystemId, Versioned, V2};
use mavspec_rust_spec::{IntoPayload, SpecError};
use prost::Message;
use std::convert::Into;
use std::string::ToString;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tonic::codegen::tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;
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
    Mission,
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

impl TypedAttribute for Mission {
    fn attribute_name() -> &'static str {
        "mavlink/mission"
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
            AttributeTypes::Mission => "mavlink/mission",
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
        CreateAttributeTypeRequest {
            attribute_type: Some(AttributeType {
                symbol: AttributeTypes::Mission.as_str().to_string(),
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

impl TryFrom<messages::MissionItemInt> for pb::mavlink::MissionItem {
    type Error = SpecError;

    fn try_from(value: MissionItemInt) -> Result<Self, Self::Error> {
        let payload = value.encode(V2::version())?;
        Ok(MissionItem {
            payload: payload.bytes().to_vec(),
        })
    }
}

fn symbol_for_node(node_id: NodeId) -> String {
    format!("mavlink/id/{}:{}", node_id.system_id, node_id.component_id)
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
        attribute_store_client
            .update_protobuf_attribute_type::<Mission>(&mavlink_fdset_entity_id)
            .await?;
    }

    println!("Mavlink running...");

    println!("Server endpoints: {:?}", args.server_endpoints);
    println!("Client endpoints: {:?}", args.client_endpoints);

    let network = Network::<V2>::create_with_capacity(128);
    let mut join_set = JoinSet::new();

    for server_address in &args.server_endpoints {
        let listener = TcpListener::bind(server_address).await?;
        join_set.spawn(network.clone().accept_loop(listener));
    }
    for client_address in &args.client_endpoints {
        let socket = TcpStream::connect(client_address).await?;
        join_set.spawn(network.clone().process_tcp(socket));
    }

    join_set.spawn(publish_to_attribute_server::<GlobalPosition, _>(
        network.subscribe::<messages::GlobalPositionInt>().await,
        attribute_store_client.clone(),
    ));
    join_set.spawn(publish_to_attribute_server::<MissionCurrent, _>(
        network.subscribe::<messages::MissionCurrent>().await,
        attribute_store_client.clone(),
    ));

    let mut mavlink_client = Client::create(
        network.clone(),
        NodeId {
            system_id: args.system_id,
            component_id: args.component_id,
        },
    );
    let node_id = NodeId { system_id: 1, component_id: 1 };
    let mut mission_fetcher = MissionFetcher {
        node_id,
        mavlink_client,
        symbol_id: symbol_for_node(node_id),
        attribute_store_client: attribute_store_client.clone(),
    };
    join_set.spawn(async move {
        loop {
            mission_fetcher.update().await?;

            sleep(Duration::from_secs(5)).await;
        }
    });

    join_set.join_all().await;

    Ok(())
}

async fn publish_to_attribute_server<A: TypedAttribute, M: mavspec_rust_spec::Message>(
    mut rx: impl Stream<Item = (NodeId, M)> + Unpin,
    mut attribute_store_client: AttributeStoreClient<Channel>,
) -> anyhow::Result<()>
where
    A: From<M>,
{
    while let Some((origin, message)) = rx.next().await {
        let symbol_id = symbol_for_node(origin);
        let attribute: A = message.into();
        let _response = attribute_store_client
            .simple_update_entity(&symbol_id, attribute)
            .await?;
    }

    Ok(())
}

struct MissionFetcher {
    node_id: NodeId,
    mavlink_client: Client<V2>,
    symbol_id: String,
    attribute_store_client: AttributeStoreClient<Channel>,
}

impl MissionFetcher {
    async fn update(&mut self) -> Result<(), anyhow::Error> {
        let mission = self.mavlink_client.fetch_mission(self.node_id).await?;

        let converted: Result<Vec<MissionItem>, _> = mission
            .into_iter()
            .map(|mission_item_int| mission_item_int.try_into())
            .collect();
        let mission_proto: pb::mavlink::Mission = Mission {
            mission_items: converted.map_err(|err| format_err!("{err:?}"))?,
        };
        let _response = self.attribute_store_client
            .simple_update_entity(&self.symbol_id, mission_proto)
            .await?;
        
        Ok(())
    }
}