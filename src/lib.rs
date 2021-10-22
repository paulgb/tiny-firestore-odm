use client::get_client;
use dynamic_firestore_client::SharedFirestoreClient;
use firestore_serde::firestore::{
    precondition::ConditionType, CreateDocumentRequest, DeleteDocumentRequest, GetDocumentRequest,
    Precondition, UpdateDocumentRequest,
};
use firestore_serde::firestore::{Document, ListDocumentsRequest};
use google_authz::TokenSource;
pub use identifiers::{CollectionName, DocumentName, QualifyDocumentName};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use tokio::sync::Mutex;
use tokio_stream::Stream;
use tonic::Code;

pub mod client;
pub mod dynamic_firestore_client;
mod identifiers;

/// Represents a key/value pair, where the key (name) is a fully-qualified path to the document.
#[derive(Hash, PartialEq, Debug, Eq)]
pub struct NamedDocument<T> {
    pub name: DocumentName,
    pub value: T,
}

/// Represents a collection of documents in a Firestore database.
///
/// Documents in Firestore do not have types, but on the Rust end, we associate each collection
/// with a type. Documents are serialized into and deserialized from this type when writing/reading
/// to Firestore.
pub struct Collection<T>
where
    T: Serialize + DeserializeOwned + 'static,
{
    db: SharedFirestoreClient,
    name: CollectionName,
    _ph: PhantomData<T>,
}

type ListResponseFuture = Pin<Box<dyn Future<Output = (VecDeque<Document>, String)> + 'static>>;

/// Stream of documents returned from a Firestore list query.
pub struct ListResponse<T>
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
{
    /// The collection we are fetching from.
    collection: CollectionName,

    /// A token provided by Firestore for pagination of list results.
    page_token: Option<String>,

    /// A buffer of items returned from the server.
    items: VecDeque<Document>,

    /// True if we have fetched all of the items from the server. We may still be able to return items
    /// from the buffer. Once depleated == true && items.is_empty(), this stream is exhausted.
    depleated: bool,

    /// A shared handle to the Firestore client.
    db: SharedFirestoreClient,

    /// A handle to the future, held when we are waiting for more data from the server.
    future: Option<ListResponseFuture>,

    page_size: u32,

    order_by: String,

    _ph: PhantomData<T>,
}

impl<T> ListResponse<T>
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
{
    /// Construct a ListResponse object.
    ///
    /// Items are lazily loaded; we do not ask Firestore for a list
    /// of documents until the first document in the stream is awaited.
    fn new(collection: CollectionName, db: SharedFirestoreClient) -> Self {
        ListResponse {
            collection,
            page_token: None,
            items: VecDeque::default(),
            db,
            depleated: false,
            future: None,
            page_size: 0,
            order_by: "".to_string(),
            _ph: PhantomData::default(),
        }
    }

    pub fn with_page_size(self, page_size: u32) -> Self {
        Self {
            page_size,
            ..self
        }
    }

    pub fn with_order_by(self, order_by: &str) -> Self {
        Self {
            order_by: order_by.to_string(),
            ..self
        }
    }

    pub async fn get_page(self) -> VecDeque<Document> {
        let (docs, _) = Self::fetch_documents(
            self.collection.parent().name(),
            self.collection.leaf_name(),
            self.page_token.clone(),
            self.db.clone(),
            self.page_size,
            self.order_by,
        )
        .await;

        docs
    }

    /// Fetch a chunk of documents from the server. The future returned by this function
    /// gets stored in self.future.
    async fn fetch_documents(
        parent: String,
        collection_id: String,
        page_token: Option<String>,
        db: SharedFirestoreClient,
        page_size: u32,
        order_by: String,
    ) -> (VecDeque<Document>, String) {
        let parent = parent;
        let collection_id = collection_id;

        let mut db = db.lock().await;
        let documents = db
            .list_documents(ListDocumentsRequest {
                collection_id,
                parent,
                page_token: page_token.unwrap_or_default(),
                page_size: page_size as i32,
                order_by,

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
    type Item = NamedDocument<T>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // If depleated is true AND the items buffer is empty, we are done.
        if self.depleated && self.items.is_empty() {
            return Poll::Ready(None);
        }
        let self_mut = self.get_mut();

        // Loop because some actions cause a state change that allow us to make progress.
        loop {
            // If the items buffer is not empty, we can return a result immediately.
            if let Some(doc) = self_mut.items.pop_front() {
                let name = DocumentName::parse(&doc.name).unwrap();
                let value =
                    firestore_serde::from_document(doc).expect("Could not convert document.");

                return Poll::Ready(Some(NamedDocument { name, value }));
            }

            // If we are already waiting for a response from the server, we poll it.
            if let Some(fut) = &mut self_mut.future {
                return match fut.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        let (items, page_token) = result;

                        self_mut.page_token = if page_token.is_empty() {
                            self_mut.depleated = true;
                            None
                        } else {
                            Some(page_token)
                        };
                        self_mut.items = items;
                        self_mut.future = None;
                        continue;
                    }
                };
            }

            // Store a future for the remaining documents. It will be polled when the loop continues.
            let fut = Box::pin(Self::fetch_documents(
                self_mut.collection.parent().name(),
                self_mut.collection.leaf_name(),
                self_mut.page_token.clone(),
                self_mut.db.clone(),
                self_mut.page_size,
                self_mut.order_by.to_string(),
            ));

            self_mut.future = Some(fut);
        }
    }
}

/// Represents a Firestore database.
pub struct Database {
    client: SharedFirestoreClient,
    project_id: String,
}

impl Database {
    pub async fn new(token_source: TokenSource, project_id: &str) -> Self {
        let client = Arc::new(Mutex::new(get_client(token_source).await.unwrap()));
        Database {
            client,
            project_id: project_id.to_string(),
        }
    }

    pub fn new_from_client(client: SharedFirestoreClient, project_id: &str) -> Self {
        Database {
            client,
            project_id: project_id.to_string(),
        }
    }

    /// Returns a top-level collection from this database.
    pub fn collection<T>(&self, name: &str) -> Collection<T>
    where
        T: Serialize + DeserializeOwned + 'static + Unpin,
    {
        let name = CollectionName::new(&self.project_id, name);
        Collection::new(self.client.clone(), name)
    }
}

impl<T> Collection<T>
where
    T: Serialize + DeserializeOwned + Unpin,
{
    /// Construct a top-level collection (i.e. a collection whose parent is the root.)
    pub fn new(db: SharedFirestoreClient, name: CollectionName) -> Self {
        Collection {
            db,
            name,
            _ph: PhantomData::default(),
        }
    }

    /// Returns a stream of all of the documents in a collection (as [NamedDocument]s).
    pub fn list(&self) -> ListResponse<T> {
        ListResponse::new(self.name.clone(), self.db.clone())
    }

    pub fn name(&self) -> CollectionName {
        self.name.clone()
    }

    pub fn subcollection<S>(&self, name: &str, collection: &str) -> Collection<S>
    where
        S: Serialize + DeserializeOwned + Unpin,
    {
        Collection {
            db: self.db.clone(),
            name: self.name.subcollection(name, collection),
            _ph: PhantomData::default(),
        }
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

        document.name = key.qualify(&self.name)?.name();
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
        document.name = key.qualify(&self.name)?.name();
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
    pub async fn create(&self, ob: &T) -> anyhow::Result<DocumentName> {
        let document = firestore_serde::to_document(ob)?;
        let result = self
            .db
            .lock()
            .await
            .create_document(CreateDocumentRequest {
                document: Some(document),
                collection_id: self.name.leaf_name(),
                parent: self.name.parent().name(),
                ..CreateDocumentRequest::default()
            })
            .await?
            .into_inner();
        Ok(DocumentName::parse(&result.name)?)
    }

    /// Overwrite the given document to this collection, creating a new document if one does not exist.
    pub async fn upsert(&self, ob: &T, key: impl QualifyDocumentName) -> anyhow::Result<()> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = key.qualify(&self.name)?.name();
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

    /// Update the given document, returning an error if it does not exist.
    pub async fn update(&self, ob: &T, key: impl QualifyDocumentName) -> anyhow::Result<()> {
        let mut document = firestore_serde::to_document(ob)?;
        document.name = key.qualify(&self.name)?.name();
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

    /// Get the document with a given key.
    pub async fn get(&self, key: impl QualifyDocumentName) -> anyhow::Result<T> {
        let document = self
            .db
            .lock()
            .await
            .get_document(GetDocumentRequest {
                name: key.qualify(&self.name)?.name(),
                ..GetDocumentRequest::default()
            })
            .await?
            .into_inner();

        firestore_serde::from_document(document)
            .map_err(|_| anyhow::anyhow!("Error deserializing."))
    }

    /// Delete the document with a given key.
    pub async fn delete(&self, key: impl QualifyDocumentName) -> anyhow::Result<()> {
        let name = key.qualify(&self.name)?.name();
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
