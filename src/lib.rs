pub use collection::Collection;
pub use database::Database;
pub use identifiers::{CollectionName, DocumentName, QualifyDocumentName};

pub mod client;
mod collection;
mod database;
pub mod dynamic_firestore_client;
mod identifiers;
mod list_response;

/// Represents a key/value pair, where the key (name) is a fully-qualified path to the document.
#[derive(Hash, PartialEq, Debug, Eq)]
pub struct NamedDocument<T> {
    pub name: DocumentName,
    pub value: T,
}
