use google_authz::{Credentials, TokenSource};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
use tiny_firestore_odm::{get_client, Collection, ObjectWithMetadata};
use tokio::sync::Mutex;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

use tonic::codegen::{Body, StdError};

const SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];

#[derive(Serialize, Deserialize)]
struct User {
    pub name: String,
    pub email: String,
    pub id: u32,
    pub city: Option<String>,
}

#[derive(Deserialize)]
struct ProjectIdExtractor {
    project_id: String,
}

async fn empty_collection<T, K>(collection: &Collection<T, K>)
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
    K: tonic::client::GrpcService<tonic::body::BoxBody> + 'static,
    K::ResponseBody: Body + Send + Sync + 'static,
    K::Error: Into<StdError>,
    <K::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    let items: Vec<ObjectWithMetadata<T>> = collection.list().collect().await;

    for item in items {
        collection.delete(&item.name).await.unwrap();
    }

    todo!()
}

fn get_source_and_project(filename: &str) -> (TokenSource, String) {
    let source: TokenSource = Credentials::from_file(filename, SCOPES).into();
    let project: ProjectIdExtractor = serde_json::from_str(
        &std::fs::read_to_string(filename).expect("Could not read credentials file"),
    )
    .expect("Could not parse credentials file");

    (source, project.project_id)
}

#[tokio::test]
async fn do_test() {
    let (source, project_id) = get_source_and_project("credentials.json");
    let client = Arc::new(Mutex::new(get_client(source).await.unwrap()));
    let users: Collection<User, _> = Collection::new(client, "users", &project_id);
    empty_collection(&users).await;

    let u1 = User {
        name: "Bob".to_string(),
        email: "bob@email".to_string(),
        id: 3,
        city: None,
    };

    let result = users.create(&u1).await.expect("Error creating user.");
    assert!(result.len() > 0);
}
