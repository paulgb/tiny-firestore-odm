use anyhow::Result;
use dynamic_firestore_client::{DynamicFirestoreClient, SharedFirestoreClient, WrappedService};
use firestore_serde::firestore::{
    firestore_client::FirestoreClient, precondition::ConditionType, CreateDocumentRequest,
    DeleteDocumentRequest, GetDocumentRequest, Precondition, UpdateDocumentRequest,
};
use firestore_serde::firestore::{Document, ListDocumentsRequest};
use googapis::CERTIFICATES;
use google_authz::{AddAuthorization, Credentials, TokenSource};
use hyper::Uri;
use qualified_document_name::{QualifiedDocumentName, QualifyDocumentName};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;
use tokio_stream::Stream;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};
use tonic::Code;

mod dynamic_firestore_client;
mod qualified_document_name;

const FIRESTORE_API_DOMAIN: &str = "firestore.googleapis.com";

pub struct Collection<T>
where
    T: Serialize + DeserializeOwned + 'static,
{
    db: SharedFirestoreClient,
    collection_id: String,
    parent: String,
    path: String,
    _ph: PhantomData<T>,
}

#[derive(Hash, PartialEq, Debug, Eq)]
pub struct ObjectWithMetadata<T> {
    pub name: QualifiedDocumentName,
    pub value: T,
}

pub struct ListResponse<T>
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
{
    parent: String,
    collection_id: String,
    page_token: Option<String>,
    items: VecDeque<Document>,
    depleated: bool,
    db: SharedFirestoreClient,
    future: Option<Pin<Box<dyn Future<Output = (VecDeque<Document>, String)> + 'static>>>,

    _ph: PhantomData<T>,
}

impl<T> ListResponse<T>
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
{
    pub fn new(parent: String, collection_id: String, db: SharedFirestoreClient) -> Self {
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
        db: SharedFirestoreClient,
    ) -> (VecDeque<Document>, String) {
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

        let documents = documents.into_inner();
        let page_token = documents.next_page_token;
        (documents.documents.into_iter().collect(), page_token)
    }
}

impl<T> Stream for ListResponse<T>
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
{
    type Item = ObjectWithMetadata<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.depleated && self.items.is_empty() {
            return Poll::Ready(None);
        }
        let self_mut = self.get_mut();

        loop {
            if let Some(doc) = self_mut.items.pop_front() {
                let name = todo!();
                let value =
                    firestore_serde::from_document(doc).expect("Could not convert document.");

                return Poll::Ready(Some(ObjectWithMetadata { name, value }));
            }

            if let Some(fut) = &mut self_mut.future {
                return match fut.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        let (items, page_token) = result;

                        self_mut.page_token = if page_token.is_empty() {
                            self_mut.depleated = true;
                            Some(page_token)
                        } else {
                            None
                        };
                        self_mut.items = items;
                        self_mut.future = None;
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
        }
    }
}

#[allow(unused)]
impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned + Unpin,
{
    pub fn new(db: SharedFirestoreClient, collection_id: &str, project_id: &str) -> Self {
        let parent = format!("projects/{}/databases/(default)/documents", project_id);
        let path = format!("{}/{}", parent, collection_id);

        Collection {
            db,
            collection_id: collection_id.to_string(),
            path,
            parent,
            _ph: PhantomData::default(),
        }
    }

    pub fn list(&self) -> ListResponse<T> {
        ListResponse::new(
            self.parent.clone(),
            self.collection_id.clone(),
            self.db.clone(),
        )
    }

    /// Create the given document in this collection with the given key.
    /// Returns an error if the key is already in use (if you intend to replace the
    /// document in that case, use `upsert` instead.)
    pub async fn create_with_key(
        &self,
        ob: &T,
        key: impl QualifyDocumentName,
    ) -> anyhow::Result<()> {
        let mut document = firestore_serde::to_document(ob)?;

        document.name = key.qualify(&self.path).name();
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
    pub async fn try_create(&self, ob: &T, key: impl QualifyDocumentName) -> anyhow::Result<bool> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = key.qualify(&self.path).name();
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
    pub async fn create(&self, ob: &T) -> anyhow::Result<QualifiedDocumentName> {
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
        Ok(todo!())
    }

    pub async fn upsert(&self, ob: &T, key: impl QualifyDocumentName) -> anyhow::Result<()> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = key.qualify(&self.path).name();
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

    pub async fn update(&self, ob: &T, key: impl QualifyDocumentName) -> anyhow::Result<()> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = key.qualify(&self.path).name();
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

    pub async fn get(&self, key: impl QualifyDocumentName) -> anyhow::Result<T> {
        let document = self
            .db
            .lock()
            .await
            .get_document(GetDocumentRequest {
                name: key.qualify(&self.path).name(),
                ..GetDocumentRequest::default()
            })
            .await?
            .into_inner();

        firestore_serde::from_document(document)
            .map_err(|_| anyhow::anyhow!("Error deserializing."))
    }

    pub async fn delete(&self, key: impl QualifyDocumentName) -> anyhow::Result<()> {
        let name = key.qualify(&self.path).name();
        self.db
            .lock()
            .await
            .delete_document(DeleteDocumentRequest {
                name,
                current_document: Some(Precondition {
                    condition_type: Some(ConditionType::Exists(true)),
                }),
            })
            .await?;
        Ok(())
    }
}

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

pub async fn get_client_default() -> Result<DynamicFirestoreClient> {
    get_client(Credentials::default().await).await
}
