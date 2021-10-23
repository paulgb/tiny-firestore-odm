use crate::dynamic_firestore_client::SharedFirestoreClient;
use crate::identifiers::{CollectionName, DocumentName};
use crate::NamedDocument;
use firestore_serde::firestore::{Document, ListDocumentsRequest};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Poll;
use tokio_stream::Stream;

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
    pub fn new(collection: CollectionName, db: SharedFirestoreClient) -> Self {
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
        Self { page_size, ..self }
    }

    pub fn with_order_by(self, order_by: &str) -> Self {
        Self {
            order_by: order_by.to_string(),
            ..self
        }
    }

    pub async fn get_page(self) -> Vec<NamedDocument<T>> {
        let (docs, _) = Self::fetch_documents(
            self.collection.parent().name(),
            self.collection.leaf_name(),
            self.page_token.clone(),
            self.db.clone(),
            self.page_size,
            self.order_by,
        )
        .await;

        docs.into_iter()
            .map(|doc| {
                let name = DocumentName::parse(&doc.name).unwrap();
                let value =
                    firestore_serde::from_document(doc).expect("Could not convert document.");

                NamedDocument { name, value }
            })
            .collect()
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
