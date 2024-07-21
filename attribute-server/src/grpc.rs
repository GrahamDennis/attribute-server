use crate::convert::{ConversionError, IntoProto, TryFromProto};
use crate::pb;
use attribute_store::store::{
    AttributeStoreError, AttributeStoreErrorKind, CreateAttributeTypeRequest, EntityLocator,
    EntityQuery, UpdateEntityRequest, WatchEntitiesRequest,
};
use std::pin::Pin;
use thiserror::Error;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::codegen::tokio_stream::Stream;
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetails, FieldViolation, StatusExt};
use tracing::Level;

pub struct AttributeServer<T> {
    store: T,
}

impl<T: attribute_store::store::ThreadSafeAttributeStore> AttributeServer<T> {
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
                match attribute_store_error.kind {
                    AttributeStoreErrorKind::EntityNotFound(entity_locator) => Status::not_found(
                        format!("no entity found matching locator {:?}", entity_locator),
                    ),
                    AttributeStoreErrorKind::ValidationError(report) => Status::with_error_details(
                        Code::InvalidArgument,
                        "validation error",
                        ErrorDetails::with_bad_request(
                            report
                                .into_inner()
                                .into_iter()
                                .map(|(path, error)| {
                                    FieldViolation::new(path.to_string(), error.to_string())
                                })
                                .collect::<Vec<_>>(),
                        ),
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
                )
            }
        }
    }
}

#[tonic::async_trait]
impl<T: attribute_store::store::ThreadSafeAttributeStore> pb::attribute_store_server::AttributeStore
    for AttributeServer<T>
{
    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn ping(
        &self,
        request: Request<pb::PingRequest>,
    ) -> Result<Response<pb::PingResponse>, Status> {
        log::info!("Received ping request");

        let _: pb::PingRequest = request.into_inner();
        let ping_response = pb::PingResponse {};

        Ok(Response::new(ping_response))
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn create_attribute_type(
        &self,
        request: Request<pb::CreateAttributeTypeRequest>,
    ) -> Result<Response<pb::CreateAttributeTypeResponse>, Status> {
        use AttributeServerError::*;

        log::info!("Received create attribute type request");

        let create_attribute_type_request_proto = request.into_inner();
        let create_attribute_type_request =
            CreateAttributeTypeRequest::try_from_proto(create_attribute_type_request_proto)
                .map_err(ConversionError)?;

        let entity = self
            .store
            .create_attribute_type(&create_attribute_type_request)
            .await
            .map_err(AttributeStoreError)?;

        let create_attribute_type_response = pb::CreateAttributeTypeResponse {
            entity: Some(entity.into_proto()),
        };

        Ok(Response::new(create_attribute_type_response))
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn get_entity(
        &self,
        request: Request<pb::GetEntityRequest>,
    ) -> Result<Response<pb::GetEntityResponse>, Status> {
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
        let get_entity_response = pb::GetEntityResponse {
            entity: Some(entity.into_proto()),
        };

        Ok(Response::new(get_entity_response))
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn query_entities(
        &self,
        request: Request<pb::QueryEntitiesRequest>,
    ) -> Result<Response<pb::QueryEntitiesResponse>, Status> {
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

        let query_entities_response = pb::QueryEntitiesResponse {
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
        request: Request<pb::UpdateEntityRequest>,
    ) -> Result<Response<pb::UpdateEntityResponse>, Status> {
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

        let update_entity_response = pb::UpdateEntityResponse {
            entity: Some(updated_entity.into_proto()),
        };

        Ok(Response::new(update_entity_response))
    }

    type WatchEntitiesStream =
        Pin<Box<dyn Stream<Item = Result<pb::WatchEntitiesEvent, Status>> + Send + 'static>>;

    #[tracing::instrument(skip(self), err(level = Level::WARN))]
    async fn watch_entities(
        &self,
        request: Request<pb::WatchEntitiesRequest>,
    ) -> Result<Response<Self::WatchEntitiesStream>, Status> {
        use AttributeServerError::*;

        log::info!("Received watch entities request");

        let watch_entities_request_proto = request.into_inner();
        let _ = WatchEntitiesRequest::try_from_proto(watch_entities_request_proto)
            .map_err(ConversionError)?;

        let receiver = self.store.watch_entities_receiver();

        let entities_stream = BroadcastStream::new(receiver).filter_map(|v| v.ok());
        let response_stream = entities_stream.map(|event| Ok(event.into_proto()));

        Ok(Response::new(Box::pin(response_stream)))
    }
}
