use std::sync::Arc;

use firestore_serde::firestore::firestore_client::FirestoreClient;
use http::Request;
use hyper::Body;
use tokio::sync::Mutex;
use tonic::{body::BoxBody, client::GrpcService, transport::channel::ResponseFuture};
use tower_service::Service;

/// A Service which uses dynamic dispatch on another service.
///
/// This mainly prevents the service type from polluting everything else.
pub struct WrappedService {
    service: Box<
        dyn Service<
            Request<BoxBody>,
            Response = http::Response<hyper::Body>,
            Error = tonic::transport::Error,
            Future = ResponseFuture,
        > + Send,
    >,
}

impl WrappedService {
    pub fn new<T>(service: T) -> Self
    where
        T: Service<
                Request<BoxBody>,
                Response = http::Response<hyper::Body>,
                Error = tonic::transport::Error,
                Future = ResponseFuture,
            > + 'static + Send,
    {
        WrappedService {
            service: Box::new(service),
        }
    }
}

impl GrpcService<BoxBody> for WrappedService {
    type ResponseBody = Body;

    type Error = tonic::transport::Error;

    type Future = ResponseFuture;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        GrpcService::poll_ready(&mut self.service, cx)
    }

    fn call(&mut self, request: Request<BoxBody>) -> Self::Future {
        GrpcService::call(&mut self.service, request)
    }
}

pub type DynamicFirestoreClient = FirestoreClient<WrappedService>;

pub type SharedFirestoreClient = Arc<Mutex<DynamicFirestoreClient>>;
