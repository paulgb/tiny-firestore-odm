use google_authz::TokenSource;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::client::get_client;
use crate::dynamic_firestore_client::SharedFirestoreClient;
use crate::{Collection, CollectionName};

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
