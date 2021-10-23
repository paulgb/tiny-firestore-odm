use crate::dynamic_firestore_client::SharedFirestoreClient;
use crate::identifiers::{CollectionName, DocumentName, QualifyDocumentName};
use crate::list_response::ListResponse;
use firestore_serde::firestore::{
    precondition::ConditionType, CreateDocumentRequest, DeleteDocumentRequest, GetDocumentRequest,
    Precondition, UpdateDocumentRequest,
};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use tonic::Code;

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
