use attribute_grpc_api::grpc::{PingRequest, PingResponse};
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Default)]
pub struct AttributeServer {}

#[tonic::async_trait]
impl attribute_grpc_api::grpc::attribute_server::Attribute for AttributeServer {
    #[tracing::instrument(skip(self))]
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("Received ping request");

        let _: PingRequest = request.into_inner();
        let ping_response = PingResponse {};

        Ok(Response::new(ping_response))
    }
}
