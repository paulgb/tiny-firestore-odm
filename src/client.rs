use crate::dynamic_firestore_client::{DynamicFirestoreClient, WrappedService};
use anyhow::Result;
use firestore_serde::firestore::firestore_client::FirestoreClient;
use googapis::CERTIFICATES;
use google_authz::{AddAuthorization, Credentials, TokenSource};
use http::Uri;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

const FIRESTORE_API_DOMAIN: &str = "firestore.googleapis.com";

/// Construct a client from a given TokenSource.
pub async fn get_client(source: impl Into<TokenSource>) -> Result<DynamicFirestoreClient> {
    let tls_config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CERTIFICATES))
        .domain_name(FIRESTORE_API_DOMAIN);

    let base_url = Uri::builder()
        .scheme("https")
        .authority(FIRESTORE_API_DOMAIN)
        .path_and_query("")
        .build()?;

    let channel = Channel::builder(base_url)
        .tls_config(tls_config)?
        .connect()
        .await?;

    let authorized_channel = AddAuthorization::init_with(source, channel);

    let client = FirestoreClient::new(WrappedService::new(authorized_channel));

    Ok(client)
}

/// Construct a client using google-authz's default credential discovery process.
pub async fn get_client_default() -> Result<DynamicFirestoreClient> {
    get_client(Credentials::default().await).await
}
