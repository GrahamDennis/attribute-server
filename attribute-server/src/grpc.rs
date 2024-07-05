use crate::convert::{ConversionError, IntoProto, TryFromProto};
use attribute_grpc_api::grpc::{
    CreateAttributeTypeRequest, CreateAttributeTypeResponse, GetEntityRequest, GetEntityResponse,
    PingRequest, PingResponse, QueryEntitiesRequest, QueryEntitiesResponse,
};
use attribute_store::store::{AttributeStoreError, AttributeType, EntityLocator, EntityQuery};
use thiserror::Error;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, StatusExt};
use tracing::Level;

pub struct AttributeServer<T> {
    store: T,
}

impl<T: attribute_store::store::AttributeStore> AttributeServer<T> {
    pub fn new(store: T) -> Self {
        AttributeServer { store }
    }
}

#[derive(Error, Debug)]
pub enum AttributeServerError {
    #[error("attribute store error")]
    AttributeStoreError(#[from] AttributeStoreError),
    #[error("conversion error")]
    ConversionError(#[from] ConversionError),
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
            AttributeServerError::ConversionError(conversion_error) => Status::with_error_details(
                Code::InvalidArgument,
                "conversion error",
                ErrorDetails::with_bad_request_violation("field", conversion_error.to_string()),
            ),
        }
    }
}

#[tonic::async_trait]
impl<T: attribute_store::store::AttributeStore>
    attribute_grpc_api::grpc::attribute_store_server::AttributeStore for AttributeServer<T>
{
    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        log::info!("Received ping request");

        let _: PingRequest = request.into_inner();
        let ping_response = PingResponse {};

        Ok(Response::new(ping_response))
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn create_attribute_type(
        &self,
        request: Request<CreateAttributeTypeRequest>,
    ) -> Result<Response<CreateAttributeTypeResponse>, Status> {
        use AttributeServerError::*;

        log::info!("Received create attribute type request");

        let create_attribute_type_request = request.into_inner();
        let attribute_type = AttributeType::try_from_proto(create_attribute_type_request)
            .map_err(ConversionError)?;

        let entity = self
            .store
            .create_attribute_type(&attribute_type)
            .await
            .map_err(AttributeStoreError)?;

        let create_attribute_type_response = CreateAttributeTypeResponse {
            entity: Some(entity.into_proto()),
        };

        Ok(Response::new(create_attribute_type_response))
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn get_entity(
        &self,
        request: Request<GetEntityRequest>,
    ) -> Result<Response<GetEntityResponse>, Status> {
        use AttributeServerError::*;

        log::info!("Received get entity request");

        let get_entity_request = request.into_inner();
        let entity_locator =
            EntityLocator::try_from_proto(get_entity_request).map_err(ConversionError)?;

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

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn query_entities(
        &self,
        request: Request<QueryEntitiesRequest>,
    ) -> Result<Response<QueryEntitiesResponse>, Status> {
        use AttributeServerError::*;

        log::info!("Received get entity request");

        let query_entities_request = request.into_inner();
        let entity_query =
            EntityQuery::try_from_proto(query_entities_request).map_err(ConversionError)?;

        let entity_rows = self
            .store
            .query_entities(&entity_query)
            .await
            .map_err(AttributeStoreError)?;
        let query_entities_response = QueryEntitiesResponse {
            // rows: entities.
            rows: entity_rows
                .into_iter()
                .map(|entity_row| entity_row.into_proto())
                .collect(),
        };

        Ok(Response::new(query_entities_response))
    }
}
