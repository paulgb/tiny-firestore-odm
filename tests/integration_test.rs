use google_authz::{Credentials, TokenSource};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
use std::{collections::HashSet, fs::read_to_string};
use tiny_firestore_odm::{Collection, CollectionName, NamedDocument, get_client};
use tokio::sync::Mutex;
use tokio_stream::StreamExt;

const SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];

#[derive(Serialize, Deserialize, PartialEq, Debug, Eq, Hash, Clone)]
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

async fn empty_collection<T>(collection: &Collection<T>)
where
    T: Serialize + DeserializeOwned + Unpin + 'static,
{
    let items: Vec<NamedDocument<T>> = collection.list().collect().await;

    for item in items {
        collection.delete(&item.name).await.unwrap();
    }
}

fn get_source_and_project() -> (TokenSource, String) {
    let creds_json = if let Ok(creds) = std::env::var("GCP_SERVICE_ACCOUNT") {
        creds
    } else {
        read_to_string("credentials.json")
            .expect("No credentials.json and GCP_SERVICE_ACCOUNT not set.")
    };

    let source: TokenSource = Credentials::from_json(creds_json.as_bytes(), SCOPES).into();
    let project: ProjectIdExtractor =
        serde_json::from_str(&creds_json).expect("Could not parse credentials file");

    (source, project.project_id)
}

#[tokio::test]
async fn do_test() {
    let (source, project_id) = get_source_and_project();
    let client = Arc::new(Mutex::new(get_client(source).await.unwrap()));
    let users: Collection<User> = Collection::new(client, CollectionName::new(&project_id, "users"));

    // Delete existing documents to create fresh start.
    empty_collection(&users).await;

    println!("h4");

    // Create a pair of users.
    let mut u1 = User {
        name: "Bob".to_string(),
        email: "bob@email".to_string(),
        id: 3,
        city: None,
    };

    let u1_key = users.create(&u1).await.expect("Error creating user.");

    let u2 = User {
        name: "Alice".to_string(),
        email: "alice@email".to_string(),
        id: 4,
        city: None,
    };

    let u2_key = users.create(&u2).await.expect("Error creating user.");

    // Fetch users and check that results match expectations.
    let users_list: Vec<NamedDocument<User>> = users.list().collect().await;
    let users_list: HashSet<NamedDocument<User>> = users_list.into_iter().collect();

    let mut expected: HashSet<NamedDocument<User>> = HashSet::new();

    expected.insert(NamedDocument {
        name: u1_key.clone(),
        value: u1.clone(),
    });

    expected.insert(NamedDocument {
        name: u2_key,
        value: u2,
    });

    assert_eq!(expected, users_list);

    // Modify Bob's email
    u1.email = "bob.albert@email".to_string();

    users.update(&u1, &u1_key).await.unwrap();

    // Fetch updated Bob
    let u1_updated = users.get(&u1_key).await.unwrap();

    assert_eq!(u1, u1_updated);
}
