# `tiny-firestore-odm`

[![wokflow state](https://github.com/paulgb/tiny-firestore-odm/workflows/Rust/badge.svg)](https://github.com/paulgb/tiny-firestore-odm/actions/workflows/rust.yml)
[![crates.io](https://img.shields.io/crates/v/tiny-firestore-odm.svg)](https://crates.io/crates/tiny-firestore-odm)

`tiny-firestore-odm` is a lightweight Object Document Mapper for Firestore. It's a lightweight
layer on top of [`firestore-serde`](https://github.com/paulgb/firestore-serde), which does the
document/object translation, adding a Rust representation of Firestore "collections" along with
methods to create/modify/delete them.

The intent is not to provide access to all of Firestore's functionality, but to provide a
simplified interface centered around using Firestore as a key/value store for arbitrary
collections of (serializable) Rust objects.

See [Are We Google Cloud Yet?](https://github.com/paulgb/are-we-google-cloud-yet) for a compatible Rust/GCP stack.

## Usage

```rust
use google_authz::Credentials;
use tiny_firestore_odm::{Collection, Database, NamedDocument};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

// Define our data model.
// Any Rust type that implements Serialize and Deserialize can be stored in a Collection.

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct ActorRole {
    actor: String,
    role: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Movie {
    pub name: String,
    pub year: u32,
    pub runtime: u32,
    pub cast: Vec<ActorRole>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Use `google-authz` for credential discovery.
    let creds = Credentials::default().await;
    // Firestore databases are namespaced by project ID, so we need that too.
    let project_id = std::env::var("GCP_PROJECT_ID").expect("Expected GCP_PROJECT_ID env var.");

    // A Database is the main wrapper around a raw FirestoreClient.
    // It gives us a way to create Collections.
    let database = Database::new(creds.into(), &project_id).await;

    // A Collection is a reference to a Firestore collection, combined with a type.
    let movies: Collection<Movie> = database.collection("movies");

    // Construct a movie to insert into our collection.
    let movie = Movie {
        name: "The Big Lebowski".to_string(),
        year: 1998,
        runtime: 117,
        cast: vec![
            ActorRole {
                actor: "Jeff Bridges".to_string(),
                role: "The Dude".to_string(),
            },
            ActorRole {
                actor: "John Goodman".to_string(),
                role: "Walter Sobchak".to_string(),
            },
            ActorRole {
                actor: "Julianne Moore".to_string(),
                role: "Maude Lebowski".to_string(),
            },
        ]
    };

    // Save the movie to the collection. When we insert a document with `create`, it is assigned
    // a random key which is returned to us if it is created successfully.
    let movie_id = movies.create(&movie).await.unwrap();

    // We can use the key that was returned to fetch the film.
    let movie_copy = movies.get(&movie_id).await.unwrap();
    assert_eq!(movie, movie_copy);

    // Alternatively, we can use our own string as the key, like this:
    movies.try_create(&movie, "The Big Lebowski").await.unwrap();

    // Then, we can retrieve it with the same string.
    let movie_copy2 = movies.get("The Big Lebowski").await.unwrap();
    assert_eq!(movie, movie_copy2);

    // To clean up, let's loop over documents in the collection and delete them.
    let mut result = movies.list();

    // List returns a `futures_core::Stream` of `NamedDocument` objects.
    while let Some(NamedDocument {name, ..}) = result.next().await {
        movies.delete(&name).await.unwrap();
    }
}
```

## Document Existence Semantics

Different methods are provided to achieve different semantics around what to do if the document
does or doesn't exist, summarized in the table below.

| Method            | Behavior if object exists      | Behavior if object does not exist |
| ----------------- | ------------------------------ | --------------------------------- |
| `create`          | N/A (picks new key)            | Create                            |
| `create_with_key` | Error                          | Create                            |
| `try_create`      | Do nothing; return `Ok(false)` | Create; return `Ok(true)`         |
| `upsert`          | Replace                        | Create                            |
| `update`          | Replace                        | Error                             |
| `delete`          | Delete                         | Error                             |

## Limitations

This crate is designed for workflows that treat Firestore as a key/value store, with each
collection corresponding to one Rust type (though one Rust type may correspond to multiple
Firestore collections).

It currently does not support functionality outside of that, including:
- Querying by anything except key
- Updating only part of a document
- Subscribing to updates

(I haven't ruled out supporting any of those features, but the goal is crate is not to
comprehensively support all GCP features, just a small but useful subset.)