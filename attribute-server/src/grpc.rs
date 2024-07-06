use crate::convert::{ConversionError, IntoProto, TryFromProto};
use attribute_grpc_api::grpc;
use attribute_grpc_api::grpc::{
    CreateAttributeTypeRequest, CreateAttributeTypeResponse, GetEntityRequest, GetEntityResponse,
    PingRequest, PingResponse, QueryEntitiesRequest, QueryEntitiesResponse, UpdateEntityResponse,
};
use attribute_store::store::{
    AttributeStoreError, AttributeType, EntityLocator, EntityQuery, UpdateEntityRequest,
};
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
                    err => Status::invalid_argument(format!("{:#}", anyhow::Error::from(err))),
                }
            }
            AttributeServerError::ConversionError(conversion_error) => {
                let ConversionError::InField(path, field_error) = conversion_error;
                let field = path.to_string();
                let field_error_message = format!("{:#}", anyhow::Error::from(field_error));
                Status::with_error_details(
                    Code::InvalidArgument,
                    "conversion error",
                    ErrorDetails::with_bad_request_violation(field, field_error_message),
            )},
        }
    }
}

#[tonic::async_trait]
impl<T: attribute_store::store::AttributeStore>
    grpc::attribute_store_server::AttributeStore for AttributeServer<T>
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

        log::info!("Received query entities request");

        let query_entities_request = request.into_inner();
        let entity_query =
            EntityQuery::try_from_proto(query_entities_request).map_err(ConversionError)?;

        let entity_rows = self
            .store
            .query_entities(&entity_query)
            .await
            .map_err(AttributeStoreError)?;

        let query_entities_response = QueryEntitiesResponse {
            rows: entity_rows
                .into_iter()
                .map(|entity_row| entity_row.into_proto())
                .collect(),
        };

        Ok(Response::new(query_entities_response))
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn update_entity(
        &self,
        request: Request<grpc::UpdateEntityRequest>,
    ) -> Result<Response<UpdateEntityResponse>, Status> {
        use AttributeServerError::*;

        log::info!("Received update entity request");

        let update_entity_request_proto = request.into_inner();
        let update_entity_request =
            UpdateEntityRequest::try_from_proto(update_entity_request_proto)
                .map_err(ConversionError)?;

        let updated_entity = self
            .store
            .update_entity(&update_entity_request)
            .await
            .map_err(AttributeStoreError)?;

        let update_entity_response = UpdateEntityResponse {
            entity: Some(updated_entity.into_proto()),
        };

        Ok(Response::new(update_entity_response))
    }
}
