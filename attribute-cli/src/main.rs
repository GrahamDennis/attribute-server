mod attributes;
mod control_loop;
mod fmt;
mod json;
mod mavlink;
mod pb;

use crate::control_loop::control_loop;
use crate::fmt::{wrap_watch_entity_rows_event, ColumnMetadata, EntityRowMetadata};
use crate::mavlink::{mavlink_run, AttributeTypes, MavlinkArgs};
use crate::pb::attribute_store_client::AttributeStoreClient;
use crate::pb::attribute_value::AttributeValue;
use crate::pb::{
    CreateAttributeTypeRequest, EntityQueryNode, PingRequest, QueryEntityRowsRequest,
    UpdateEntityRequest, WatchEntitiesRequest, WatchEntityRowsRequest,
};
use anyhow::format_err;
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::Shell;
use prost_reflect::{DescriptorPool, ReflectMessage};
use serde::Deserializer;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::future::Future;
use thiserror::Error;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::Status;
use tonic_types::{ErrorDetail, StatusExt};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Endpoint to connect to
    #[arg(short, long, default_value = "http://[::1]:50051")]
    endpoint: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Send ping request
    Ping,
    /// Create attribute
    CreateAttributeType {
        #[clap(short, long)]
        json: String,
    },
    /// Query for entities
    QueryEntityRows {
        #[clap(short, long)]
        json: String,
    },
    /// Update entity
    UpdateEntity {
        #[clap(short, long)]
        json: String,
    },
    /// Watch for changes to entities
    WatchEntities {
        #[clap(short, long)]
        json: String,
    },
    /// Watch for changes to entity rows
    WatchEntityRows {
        #[clap(short, long)]
        json: String,
    },
    ControlLoop {},
    Mavlink(MavlinkArgs),
    /// Generate shell completions script
    GenerateCompletions {
        /// shell to generate completions for
        #[clap(short, long)]
        shell: Option<Shell>,
    },
}

#[derive(Error, Debug)]
pub struct StatusError {
    status: Status,
    error_details: Vec<ErrorDetail>,
}

impl From<Status> for StatusError {
    /// Convert a grpc Status into something that we can display a better error for
    fn from(value: Status) -> Self {
        let error_details = value.get_error_details_vec();
        StatusError {
            status: value,
            error_details,
        }
    }
}

impl Display for StatusError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "status: {:?}, message: {:?}, metadata: {:?}",
            self.status.code(),
            self.status.message(),
            self.status.metadata(),
        )?;

        if !self.error_details.is_empty() {
            write!(f, "\nDetails:")?;
            for error_detail in self.error_details.iter() {
                write!(f, "\n * {:?}", error_detail)?;
            }
        }

        Ok(())
    }
}

fn print_completions<G: clap_complete::Generator>(gen: G, cmd: &mut clap::Command) {
    clap_complete::generate(gen, cmd, cmd.get_name().to_string(), &mut std::io::stdout());
}

async fn send_request<T: ReflectMessage + Default, R: ReflectMessage, Fut>(
    json: &str,
    call: impl FnOnce(T) -> Fut,
) -> anyhow::Result<()>
where
    Fut: Future<Output = Result<tonic::Response<R>, Status>>,
{
    let request: T = json::parse_from_json_argument(json)?;

    let response = call(request).await.map_err(StatusError::from)?;
    let response = response.into_inner();
    println!("{}", json::to_json(&response)?);

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        Commands::Ping => {
            let mut attribute_store_client = create_attribute_store_client(&cli.endpoint).await?;
            let response = attribute_store_client.ping(PingRequest {}).await?;
            println!("response: {:?}", response);

            Ok(())
        }
        Commands::CreateAttributeType { json } => {
            let mut client = create_attribute_store_client(&cli.endpoint).await?;
            send_request(json, |request: CreateAttributeTypeRequest| {
                client.create_attribute_type(request)
            })
            .await
        }
        Commands::QueryEntityRows { json } => {
            let mut client = create_attribute_store_client(&cli.endpoint).await?;
            send_request(json, |request: QueryEntityRowsRequest| {
                client.query_entity_rows(request)
            })
            .await
        }
        Commands::UpdateEntity { json } => {
            let mut client = create_attribute_store_client(&cli.endpoint).await?;
            send_request(json, |request: UpdateEntityRequest| {
                client.update_entity(request)
            })
            .await
        }
        Commands::WatchEntities { json } => {
            let request: WatchEntitiesRequest = json::parse_from_json_argument(json)?;

            let mut attribute_store_client = create_attribute_store_client(&cli.endpoint).await?;
            let response = attribute_store_client
                .watch_entities(request)
                .await
                .map_err(StatusError::from)?;
            let mut stream = response.into_inner();
            while let Some(event) = stream.message().await? {
                println!("{}", json::to_json(&event)?);
            }

            Ok(())
        }
        Commands::WatchEntityRows { json } => {
            let request: WatchEntityRowsRequest = json::parse_from_json_argument(json)?;

            let mut attribute_store_client = create_attribute_store_client(&cli.endpoint).await?;

            let protobuf_metadata_attribute_types = vec![
                "@symbolName".to_string(),
                AttributeTypes::FileDescriptorSetRef.as_str().to_string(),
                AttributeTypes::MessageName.as_str().to_string(),
            ];
            let file_descriptor_set_attribute_types = vec![
                "@id".to_string(),
                AttributeTypes::FileDescriptorSet.as_str().to_string(),
            ];

            let query_protobuf_metadata = QueryEntityRowsRequest {
                root: Some(EntityQueryNode {
                    query: Some(pb::entity_query_node::Query::HasAttributeTypes(
                        pb::HasAttributeTypesNode {
                            attribute_types: protobuf_metadata_attribute_types.clone(),
                        },
                    )),
                }),
                attribute_types: protobuf_metadata_attribute_types.clone(),
            };

            // attribute_type => (file_descriptor_set_entity_id, message_name)
            let protobuf_metadatas: HashMap<String, (String, String)> = attribute_store_client
                .query_entity_rows(query_protobuf_metadata)
                .await?
                .into_inner()
                .rows
                .into_iter()
                .filter_map(|row| {
                    let attribute_type = row.string_value(0)?.clone();
                    let file_descriptor_set_entity_id = row.entity_id_value(1)?.clone();

                    let message_name = row.string_value(2)?.clone();

                    Some((
                        attribute_type,
                        (file_descriptor_set_entity_id, message_name),
                    ))
                })
                .collect();

            let file_descriptor_sets: HashMap<String, DescriptorPool> = attribute_store_client
                .query_entity_rows(QueryEntityRowsRequest {
                    root: Some(EntityQueryNode {
                        query: Some(pb::entity_query_node::Query::HasAttributeTypes(
                            pb::HasAttributeTypesNode {
                                attribute_types: vec![AttributeTypes::FileDescriptorSet
                                    .as_str()
                                    .to_string()],
                            },
                        )),
                    }),
                    attribute_types: file_descriptor_set_attribute_types.clone(),
                })
                .await?
                .into_inner()
                .rows
                .into_iter()
                .filter_map(|row| {
                    let entity_id = row.entity_id_value(0)?.clone();
                    let file_descriptor_set_bytes = row.bytes_value(1)?;

                    let descriptor_pool =
                        DescriptorPool::decode(file_descriptor_set_bytes.as_slice()).ok()?;

                    Some((entity_id, descriptor_pool))
                })
                .collect();

            let entity_row_metadata = EntityRowMetadata {
                columns: request
                    .attribute_types
                    .iter()
                    .map(|attribute_type| {
                        let (file_descriptor_set_entity_id, message_name) =
                            protobuf_metadatas.get(attribute_type)?;

                        let descriptor_pool =
                            file_descriptor_sets.get(file_descriptor_set_entity_id)?;
                        let message_descriptor =
                            descriptor_pool.get_message_by_name(message_name)?;
                        Some(ColumnMetadata::MessageDescriptor(message_descriptor))
                    })
                    .collect(),
            };
            let response = attribute_store_client
                .watch_entity_rows(request)
                .await
                .map_err(StatusError::from)?;

            let mut stream = response.into_inner();
            while let Some(event) = stream.message().await? {
                println!(
                    "{}",
                    json::serialize_to_json(&wrap_watch_entity_rows_event(
                        &event,
                        &entity_row_metadata
                    ))?
                );

                // println!("{}", json::to_json(&event)?);
            }

            Ok(())
        }
        Commands::GenerateCompletions { shell } => Ok(print_completions(
            shell
                .or_else(|| Shell::from_env())
                .ok_or_else(|| format_err!("specify shell with `--shell`"))?,
            &mut Cli::command(),
        )),
        Commands::ControlLoop { .. } => {
            let _ = control_loop(&cli).await?;

            Ok(())
        }
        Commands::Mavlink(mavlink_args) => {
            let _ = mavlink_run(&cli, mavlink_args).await?;

            Ok(())
        }
    }
}

async fn create_attribute_store_client(
    endpoint: &str,
) -> anyhow::Result<AttributeStoreClient<Channel>> {
    let channel = Endpoint::from_shared(endpoint.to_string())?
        .connect()
        .await?;

    Ok(AttributeStoreClient::new(channel))
}
