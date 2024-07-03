use crate::grpc::AttributeServer;
use attribute_grpc_api::grpc::attribute_store_server;
use attribute_store::inmemory::InMemoryAttributeStore;
use std::time::Duration;
use tonic::transport::Server;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

mod grpc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let addr = "[::1]:50051".parse().unwrap();

    let attribute_server = AttributeServer::new(InMemoryAttributeStore::new());

    let layer = tower::ServiceBuilder::new()
        // Apply middleware from tower
        .timeout(Duration::from_secs(30))
        .into_inner();

    info!("attribute-server listening on {}", addr);

    Server::builder()
        .layer(layer)
        .add_service(attribute_store_server::AttributeStoreServer::new(
            attribute_server,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
