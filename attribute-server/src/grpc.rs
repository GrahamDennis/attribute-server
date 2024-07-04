use crate::convert::{ConversionError, IntoProto, TryFromProto};
use attribute_grpc_api::grpc::{GetEntityRequest, GetEntityResponse, PingRequest, PingResponse};
use attribute_store::store::{AttributeStoreError, EntityLocator};
use thiserror::Error;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};
use tracing::info;

pub struct AttributeServer<T> {
    store: T,
}

impl<T: attribute_store::store::AttributeStore> AttributeServer<T> {
    pub fn new(store: T) -> Self {
        AttributeServer { store }
    }
}

impl From<ConversionError> for Status {
    fn from(value: ConversionError) -> Self {
        Status::with_error_details(
            Code::InvalidArgument,
            "conversion error",
            ErrorDetails::with_bad_request_violation("field", value.to_string()),
        )
    }
}

#[derive(Error, Debug)]
pub enum AttributeServerError {
    #[error("attribute store error")]
    AttributeStoreError(#[from] AttributeStoreError),
}

impl From<AttributeServerError> for Status {
    fn from(value: AttributeServerError) -> Self {
        match value {
            AttributeServerError::AttributeStoreError(attribute_store_error) => {
                match attribute_store_error {
                    AttributeStoreError::EntityNotFound(entity_locator) => Status::not_found(
                        format!("no entity found matching locator {:?}", entity_locator),
                    ),
                    err => Status::invalid_argument(err.to_string()),
                }
            }
        }
    }
}

#[tonic::async_trait]
impl<T: attribute_store::store::AttributeStore>
    attribute_grpc_api::grpc::attribute_store_server::AttributeStore for AttributeServer<T>
{
    #[tracing::instrument(skip(self))]
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("Received ping request");

        let _: PingRequest = request.into_inner();
        let ping_response = PingResponse {};

        Ok(Response::new(ping_response))
    }

    #[tracing::instrument(skip(self))]
    async fn get_entity(
        &self,
        request: Request<GetEntityRequest>,
    ) -> Result<Response<GetEntityResponse>, Status> {
        use AttributeServerError::*;

        info!("Received get entity request");

        let get_entity_request = request.into_inner();
        let entity_locator = EntityLocator::try_from_proto(get_entity_request)?;

        let entity = self
            .store
            .get_entity(&entity_locator)
            .await
            .map_err(AttributeStoreError)?;
        let get_entity_response = GetEntityResponse {
            entity: Some(entity.into_proto()),
        };

        Ok(Response::new(get_entity_response))
    }
}
