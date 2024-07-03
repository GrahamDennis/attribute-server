use attribute_grpc_api::grpc::{GetEntityRequest, GetEntityResponse, PingRequest, PingResponse};
use tonic::{Request, Response, Status};
use tracing::info;

pub struct AttributeServer<T> {
    store: T,
}

impl<T: attribute_store::store::AttributeStore> AttributeServer<T> {
    pub fn new(store: T) -> Self {
        AttributeServer { store }
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
        todo!()
    }
}
