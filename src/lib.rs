use anyhow::Result;
use firestore_serde::firestore::{
    firestore_client::FirestoreClient, precondition::ConditionType, CreateDocumentRequest,
    DeleteDocumentRequest, GetDocumentRequest, Precondition, UpdateDocumentRequest,
};
use firestore_serde::firestore::{Document, ListDocumentsRequest};
use googapis::CERTIFICATES;
use google_authz::{AddAuthorization, Credentials, TokenSource};
use hyper::Uri;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::{marker::PhantomData, sync::Arc};
use tokio::sync::Mutex;
use tokio_stream::Stream;
use tonic::codegen::{Body, StdError};
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Code;

const FIRESTORE_API_DOMAIN: &str = "firestore.googleapis.com";

pub struct Collection<T, K>
where
    T: Serialize + DeserializeOwned + 'static,
    K: tonic::client::GrpcService<tonic::body::BoxBody> + 'static,
    K::ResponseBody: Body + Send + Sync + 'static,
    K::Error: Into<StdError>,
    <K::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    db: Arc<Mutex<FirestoreClient<K>>>,
    collection_id: String,
    parent: String,
    _ph: PhantomData<T>,
}

pub struct ObjectWithMetadata<T> {
    pub name: String,
    pub value: T,
}

pub struct ListResponse<T, K>
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
    K: tonic::client::GrpcService<tonic::body::BoxBody> + 'static,
    K::ResponseBody: Body + Send + Sync + 'static,
    K::Error: Into<StdError>,
    <K::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    parent: String,
    collection_id: String,
    page_token: Option<String>,
    items: VecDeque<Document>,
    depleated: bool,
    db: Arc<Mutex<FirestoreClient<K>>>,
    future: Option<Pin<Box<dyn Future<Output = VecDeque<Document>> + 'static>>>,

    _ph: PhantomData<T>,
}

impl<T, K> ListResponse<T, K>
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
    K: tonic::client::GrpcService<tonic::body::BoxBody> + 'static,
    K::ResponseBody: Body + Send + Sync + 'static,
    K::Error: Into<StdError>,
    <K::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    pub fn new(parent: String, collection_id: String, db: Arc<Mutex<FirestoreClient<K>>>) -> Self {
        ListResponse {
            parent,
            collection_id,
            page_token: None,
            items: VecDeque::default(),
            db,
            depleated: false,
            future: None,
            _ph: PhantomData::default(),
        }
    }

    async fn fetch_documents(
        parent: String,
        collection_id: String,
        page_token: Option<String>,
        db: Arc<Mutex<FirestoreClient<K>>>,
    ) -> VecDeque<Document> {
        let parent = parent;
        let collection_id = collection_id;

        let mut db = db.lock().await;
        let documents = db
            .list_documents(ListDocumentsRequest {
                collection_id,
                parent,
                page_token: page_token.unwrap_or_default(),

                ..ListDocumentsRequest::default()
            })
            .await
            .unwrap();

        documents.into_inner().documents.into_iter().collect()
    }
}

impl<T, K> Stream for ListResponse<T, K>
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
    K: tonic::client::GrpcService<tonic::body::BoxBody> + 'static,
    K::ResponseBody: Body + Send + Sync + 'static,
    K::Error: Into<StdError>,
    <K::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    type Item = ObjectWithMetadata<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.depleated {
            return Poll::Ready(None);
        }
        let self_mut = self.get_mut();

        loop {
            if let Some(doc) = self_mut.items.pop_front() {
                let name = doc.name.clone();
                let value =
                    firestore_serde::from_document(doc).expect("Could not convert document.");

                return Poll::Ready(Some(ObjectWithMetadata { name, value }));
            }

            if let Some(fut) = &mut self_mut.future {
                return match fut.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(c) => {
                        self_mut.items = c;
                        continue;
                    }
                };
            }

            let fut = Box::pin(Self::fetch_documents(
                self_mut.parent.clone(),
                self_mut.collection_id.clone(),
                self_mut.page_token.clone(),
                self_mut.db.clone(),
            ));

            self_mut.future = Some(fut);
            return Poll::Pending;
        }
    }
}

#[allow(unused)]
impl<T, K> Collection<T, K>
where
    T: Serialize + DeserializeOwned + Unpin,
    K: tonic::client::GrpcService<tonic::body::BoxBody>,
    K::ResponseBody: Body + Send + Sync + 'static,
    K::Error: Into<StdError>,
    <K::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    pub fn new(db: Arc<Mutex<FirestoreClient<K>>>, collection_id: &str, project_id: &str) -> Self {
        Collection {
            db,
            collection_id: collection_id.to_string(),
            parent: format!("projects/{}/databases/(default)/documents", project_id),
            _ph: PhantomData::default(),
        }
    }

    pub fn path(&self) -> String {
        format!("{}/{}", self.parent, self.collection_id)
    }

    pub fn get_name(&self, key: &str) -> String {
        format!("{}/{}", self.path(), key)
    }

    pub fn list(&self) -> ListResponse<T, K> {
        ListResponse::new(
            self.parent.clone(),
            self.collection_id.clone(),
            self.db.clone(),
        )
    }

    /// Create the given document in this collection with the given key.
    /// Returns an error if the key is already in use (if you intend to replace the
    /// document in that case, use `upsert` instead.)
    pub async fn create_with_key(&self, ob: &T, key: &str) -> anyhow::Result<()> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = self.get_name(key);
        self.db
            .lock()
            .await
            .update_document(UpdateDocumentRequest {
                document: Some(document),
                current_document: Some(Precondition {
                    condition_type: Some(ConditionType::Exists(false)),
                }),
                ..UpdateDocumentRequest::default()
            })
            .await?;
        Ok(())
    }

    /// Create the given document in this collection with the given key.
    /// Returns `true` if the document was created, or `false` if it already existed.
    pub async fn try_create(&self, ob: &T, key: &str) -> anyhow::Result<bool> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = self.get_name(key);
        let result = self
            .db
            .lock()
            .await
            .update_document(UpdateDocumentRequest {
                document: Some(document),
                current_document: Some(Precondition {
                    condition_type: Some(ConditionType::Exists(false)),
                }),
                ..UpdateDocumentRequest::default()
            })
            .await;

        match result {
            Ok(_) => Ok(true),
            Err(e) if e.code() == Code::AlreadyExists => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Add the given document to this collection, assigning it a new key at random.
    pub async fn create(&self, ob: &T) -> anyhow::Result<String> {
        let document = firestore_serde::to_document(ob)?;
        let result = self
            .db
            .lock()
            .await
            .create_document(CreateDocumentRequest {
                document: Some(document),
                collection_id: self.collection_id.to_string(),
                parent: self.parent.to_string(),
                ..CreateDocumentRequest::default()
            })
            .await?
            .into_inner();
        Ok(result.name)
    }

    pub async fn upsert(&self, ob: &T, key: &str) -> anyhow::Result<()> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = self.get_name(key);
        self.db
            .lock()
            .await
            .update_document(UpdateDocumentRequest {
                document: Some(document),
                ..UpdateDocumentRequest::default()
            })
            .await?;
        Ok(())
    }

    pub async fn update(&self, ob: &T, key: &str) -> anyhow::Result<()> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = self.get_name(key);
        self.db
            .lock()
            .await
            .update_document(UpdateDocumentRequest {
                document: Some(document),
                current_document: Some(Precondition {
                    condition_type: Some(ConditionType::Exists(true)),
                }),
                ..UpdateDocumentRequest::default()
            })
            .await?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<T> {
        let document = self
            .db
            .lock()
            .await
            .get_document(GetDocumentRequest {
                name: self.get_name(key),
                ..GetDocumentRequest::default()
            })
            .await?
            .into_inner();

        firestore_serde::from_document(document)
            .map_err(|_| anyhow::anyhow!("Error deserializing."))
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let name = self.get_name(key);
        self.db
            .lock()
            .await
            .delete_document(DeleteDocumentRequest {
                name,
                ..DeleteDocumentRequest::default()
            })
            .await?;
        Ok(())
    }
}

pub async fn get_client(
    source: impl Into<TokenSource>,
) -> Result<FirestoreClient<AddAuthorization<Channel>>> {
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

    let client = FirestoreClient::new(authorized_channel);

    Ok(client)
}

pub async fn get_client_default() -> Result<FirestoreClient<AddAuthorization<Channel>>> {
    get_client(Credentials::default().await).await
}
