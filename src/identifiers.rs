use std::error::Error;
use std::fmt::Display;

/// Errors relating to parsing a DocumentName or CollectionName.
#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// The string does not have enough slash-delimited parts to be understood.
    TooFewParts(usize),

    /// The string has the wrong number of parts for the type it is being parsed as.
    ///
    /// Collections have an even number of parts (5 fixed + collection name).
    /// Documents have an odd number of parts (5 fixed + n * 2 (collection/name pairs)).
    WrongNumberOfParts(usize),

    /// A part of the path that was expected to be a well-known string was not.
    ///
    /// The parameter indicates the index of the offending part.
    InvalidPart(usize),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidPart(part) => write!(f, "Invalid part at index {}", part),
            ParseError::WrongNumberOfParts(parts) => write!(f, "Invalid number of parts {}", parts),
            ParseError::TooFewParts(parts) => write!(f, "Expected at least 6 parts, got {}", parts),
        }
    }
}

impl Error for ParseError {}

/// Represents the parent of a collection, which is either another document or the “root” collection.
pub enum ParentDocumentOrRoot {
    Root { project_id: String },
    ParentDocument { document: DocumentName },
}

impl ParentDocumentOrRoot {
    /// Returns a string suitable for passing in the Firestore API as a `parent` parameter.
    pub fn name(&self) -> String {
        match self {
            Self::Root { project_id } => {
                format!("projects/{}/databases/(default)/documents", project_id)
            }
            Self::ParentDocument { document } => document.name(),
        }
    }

    /// Returns the collection above this one, if it is not the root collection.
    pub fn parent(&self) -> Option<CollectionName> {
        match self {
            Self::Root { .. } => None,
            Self::ParentDocument { document } => Some(document.collection.clone()),
        }
    }
}

/// Represents the fully-qualified path of a collection.
#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct CollectionName {
    project_id: String,
    /// Vector of (collection, name) pairs of parent.
    parent_path: Vec<(String, String)>,

    /// Name of the "leaf" collection.
    collection: String,
}

impl CollectionName {
    /// Construct a `CollectionName` for a top-level collection, i.e. a direct descendent of the root.
    ///
    /// Equivalent to `new_with_path` with an empty `path`.
    pub fn new(project_id: &str, collection: &str) -> Self {
        CollectionName::new_with_path(project_id, &[], collection)
    }

    /// Construct a `CollectionName` nested under a document.
    pub fn new_with_path(project_id: &str, path: &[(&str, &str)], collection: &str) -> Self {
        let parent_path: Vec<(String, String)> = path
            .iter()
            .map(|(collection, name)| (collection.to_string(), name.to_string()))
            .collect();

        CollectionName {
            project_id: project_id.to_string(),
            parent_path,
            collection: collection.to_string(),
        }
    }

    pub fn subcollection(&self, name: &str, collection: &str) -> CollectionName {
        let mut parent_path = self.parent_path.clone();
        parent_path.push((self.collection.clone(), name.to_string()));
        CollectionName {
            collection: collection.to_string(),
            project_id: self.project_id.clone(),
            parent_path
        }
    }

    /// Return a representation of the parent of this collection, which may either be a document or the root.
    pub fn parent(&self) -> ParentDocumentOrRoot {
        let mut parent_path = self.parent_path.clone();

        if let Some((collection, name)) = parent_path.pop() {
            ParentDocumentOrRoot::ParentDocument {
                document: DocumentName {
                    collection: CollectionName {
                        project_id: self.project_id.clone(),
                        parent_path,
                        collection,
                    },
                    name,
                },
            }
        } else {
            ParentDocumentOrRoot::Root {
                project_id: self.project_id.clone(),
            }
        }
    }

    /// Return the collection of the document that this collection is nested under, if it exists.
    ///
    /// If this collection is directly under the root, returns `None`.
    pub fn parent_collection(&self) -> Option<Self> {
        self.parent().parent()
    }

    /// Return a reference to a named document directly under this collection.
    pub fn document(&self, name: &str) -> DocumentName {
        DocumentName {
            collection: self.clone(),
            name: name.to_string(),
        }
    }

    /// Returns the short name of this collection without the full path.
    pub fn leaf_name(&self) -> String {
        self.collection.clone()
    }

    /// Returns the fully-qualified name of this collection as a string.
    pub fn name(&self) -> String {
        let path = if self.parent_path.is_empty() {
            format!("documents/{}", self.collection)
        } else {
            let path_parts: Vec<String> = self
                .parent_path
                .iter()
                .map(|(collection, name)| format!("{}/{}", collection, name))
                .collect();
            let path: String = path_parts.join("/");
            format!("documents/{}/{}", path, self.collection)
        };

        format!("projects/{}/databases/(default)/{}", self.project_id, path)
    }

    /// Attempt to parse a collection name from a slash-delimited string.
    pub fn parse(name: &str) -> Result<Self, ParseError> {
        let parts: Vec<&str> = name.split('/').into_iter().collect();

        if parts.len() < 5 {
            return Err(ParseError::TooFewParts(parts.len()));
        } else if parts.len() % 2 != 0 {
            return Err(ParseError::WrongNumberOfParts(parts.len()));
        }

        if parts.get(0) != Some(&"projects") {
            return Err(ParseError::InvalidPart(0));
        }
        if parts.get(2) != Some(&"databases") {
            return Err(ParseError::InvalidPart(2));
        }
        if parts.get(3) != Some(&"(default)") {
            return Err(ParseError::InvalidPart(3));
        }
        if parts.get(4) != Some(&"documents") {
            return Err(ParseError::InvalidPart(4));
        }

        let project_id = parts.get(1).unwrap().to_string();

        let depth = (parts.len() - 6) / 2;

        let parent_path: Vec<(String, String)> = (0..depth)
            .map(|d| {
                (
                    parts.get(5 + d * 2).unwrap().to_string(),
                    parts.get(6 + d * 2).unwrap().to_string(),
                )
            })
            .collect();
        let collection = parts.last().unwrap().to_string();

        Ok(CollectionName {
            project_id,
            collection,
            parent_path,
        })
    }
}

/// Represents a fully-qualified Firestore document name.
#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct DocumentName {
    collection: CollectionName,
    name: String,
}

impl DocumentName {
    /// Returns this document name as a fully-qualified string.
    pub fn name(&self) -> String {
        format!("{}/{}", self.collection.name(), self.name)
    }

    pub fn leaf_name(&self) -> &str {
        &self.name
    }

    /// Parse a document name from a fully-qualified string.
    pub fn parse(name: &str) -> Result<Self, ParseError> {
        let (collection_name, name) = name.rsplit_once("/").unwrap();

        let collection = CollectionName::parse(collection_name).unwrap();

        Ok(DocumentName {
            collection,
            name: name.to_string(),
        })
    }
}

#[derive(Debug, PartialEq)]
pub enum QualifyError {
    ProjectMismatch(String, String),
    CollectionMismatch(CollectionName, CollectionName),
}

impl Display for QualifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProjectMismatch(expected, actual) => write!(
                f,
                "Attempted a collection-level operation with a document name from a different project. This collection has project {} but attempted operation had project {}",
                expected,
                actual),
            Self::CollectionMismatch(expected, actual) => write!(
                f,
                "Attempted to a collection-level operation with a document name from a different collection. This collection has the path {}, but attempted operation has the path {}", 
                expected.name(),
                actual.name())
        }
    }
}

impl Error for QualifyError {}

/// Represents a type that can be turned into a fully-qualified document name.
pub trait QualifyDocumentName {
    /// Create a document name from self, using the given collection as its parent.
    fn qualify(&self, parent: &CollectionName) -> Result<DocumentName, QualifyError>;
}

impl QualifyDocumentName for &str {
    fn qualify(&self, parent: &CollectionName) -> Result<DocumentName, QualifyError> {
        Ok(parent.document(self))
    }
}

impl QualifyDocumentName for &DocumentName {
    fn qualify(&self, parent: &CollectionName) -> Result<DocumentName, QualifyError> {
        if self.collection.project_id != parent.project_id {
            return Err(QualifyError::ProjectMismatch(
                self.collection.project_id.to_string(),
                parent.project_id.to_string(),
            ));
        }

        if &self.collection != parent {
            return Err(QualifyError::CollectionMismatch(
                self.collection.clone(),
                parent.clone(),
            ));
        }

        Ok((*self).clone())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_qualify() {
        let collection = CollectionName::new("my-project", "things");

        assert_eq!(
            "projects/my-project/databases/(default)/documents/things/blah",
            "blah".qualify(&collection).unwrap().name()
        );

        let doc = collection.document("mydoc");

        assert_eq!(
            "projects/my-project/databases/(default)/documents/things/mydoc",
            (&doc).qualify(&collection).unwrap().name()
        );
    }

    #[test]
    fn test_subcollection() {
        let collection = CollectionName::new("my-project", "things");

        let subcollection = collection.subcollection("thing1", "apps");

        assert_eq!(
            "projects/my-project/databases/(default)/documents/things/thing1/apps",
            subcollection.name(),
        );
    }

    #[test]
    fn test_fail_qualify() {
        let collection1 = CollectionName::new("my-project", "things");
        let collection2 = CollectionName::new("my-project", "stuff");
        let collection3 = CollectionName::new("my-other-project", "stuff");

        assert_eq!(
            QualifyError::CollectionMismatch(collection1.clone(), collection2.clone()),
            (&collection1.document("foobar"))
                .qualify(&collection2)
                .unwrap_err()
        );

        assert_eq!(
            QualifyError::ProjectMismatch("my-project".to_string(), "my-other-project".to_string()),
            (&collection1.document("foobar"))
                .qualify(&collection3)
                .unwrap_err()
        );
    }

    #[test]
    fn test_construct_document_name() {
        let collection = CollectionName::new("my-project", "things");

        assert_eq!(
            "projects/my-project/databases/(default)/documents/things",
            collection.name()
        );

        assert_eq!(
            "projects/my-project/databases/(default)/documents",
            collection.parent().name()
        );

        let doc1 = collection.document("thing1");

        assert_eq!(
            "projects/my-project/databases/(default)/documents/things/thing1",
            doc1.name()
        );
    }

    #[test]
    fn test_construct_multi_part_collection_name() {
        let collection = CollectionName::new_with_path(
            "some-project",
            &[("people", "john"), ("items", "phone")],
            "apps",
        );

        assert_eq!(
            "projects/some-project/databases/(default)/documents/people/john/items/phone/apps",
            collection.name()
        );
    }

    #[test]
    fn test_parse_collection_name() {
        let name_to_parse = "projects/employee-directory/databases/(default)/documents/people";

        let expected = CollectionName {
            collection: "people".to_string(),
            parent_path: vec![],

            project_id: "employee-directory".to_string(),
        };

        let result = CollectionName::parse(name_to_parse).unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_multi_part_collection_name() {
        let name_to_parse =
            "projects/stuff/databases/(default)/documents/people/john/items/phone/apps";

        let expected = CollectionName {
            collection: "apps".to_string(),
            parent_path: vec![
                ("people".to_string(), "john".to_string()),
                ("items".to_string(), "phone".to_string()),
            ],
            project_id: "stuff".to_string(),
        };

        let result = CollectionName::parse(name_to_parse).unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_multi_part_document_name() {
        let name_to_parse =
            "projects/stuff/databases/(default)/documents/people/john/items/phone/apps/clock";

        let expected = DocumentName {
            collection: CollectionName {
                collection: "apps".to_string(),
                parent_path: vec![
                    ("people".to_string(), "john".to_string()),
                    ("items".to_string(), "phone".to_string()),
                ],
                project_id: "stuff".to_string(),
            },
            name: "clock".to_string(),
        };

        let result = DocumentName::parse(name_to_parse).unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn test_fail_parse_collection_name() {
        assert_eq!(
            ParseError::TooFewParts(2),
            CollectionName::parse("projects/employee-directory").unwrap_err()
        );

        assert_eq!(
            ParseError::WrongNumberOfParts(7),
            CollectionName::parse(
                "projects/employee-directory/databases/(default)/documents/people/stuff"
            )
            .unwrap_err()
        );

        assert_eq!(
            ParseError::InvalidPart(0),
            CollectionName::parse(
                "project/employee-directory/databases/(default)/documents/people"
            )
            .unwrap_err()
        );

        assert_eq!(
            ParseError::InvalidPart(2),
            CollectionName::parse("projects/employee-directory/databa/(default)/documents/people")
                .unwrap_err()
        );

        assert_eq!(
            ParseError::InvalidPart(3),
            CollectionName::parse("projects/employee-directory/databases/default/documents/people")
                .unwrap_err()
        );

        assert_eq!(
            ParseError::InvalidPart(4),
            CollectionName::parse("projects/employee-directory/databases/(default)/stuff/people")
                .unwrap_err()
        );
    }

    #[test]
    fn test_parse_document_name() {
        let name_to_parse = "projects/employee-directory/databases/(default)/documents/people/jack";

        let expected = DocumentName {
            collection: CollectionName {
                collection: "people".to_string(),
                parent_path: vec![],

                project_id: "employee-directory".to_string(),
            },
            name: "jack".to_string(),
        };

        let result = DocumentName::parse(name_to_parse).unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn test_walk_to_root() {
        let name_to_parse =
            "projects/stuff/databases/(default)/documents/people/john/items/phone/apps/clock";

        let result = DocumentName::parse(name_to_parse).unwrap();

        assert_eq!(name_to_parse, &result.name());

        let result = result.collection;

        assert_eq!(
            "projects/stuff/databases/(default)/documents/people/john/items/phone/apps",
            result.name()
        );

        let result = result.parent();

        assert_eq!(
            "projects/stuff/databases/(default)/documents/people/john/items/phone",
            result.name()
        );

        let result = result.parent().unwrap();

        assert_eq!(
            "projects/stuff/databases/(default)/documents/people/john/items",
            result.name()
        );

        let result = result.parent_collection().unwrap();

        assert_eq!(
            "projects/stuff/databases/(default)/documents/people",
            result.name()
        );

        let result = result.parent();

        assert_eq!(
            "projects/stuff/databases/(default)/documents",
            result.name()
        );
    }

    #[test]
    fn test_walk_from_root() {
        let collection = CollectionName::new("my-project", "beers");

        assert_eq!(
            "projects/my-project/databases/(default)/documents/beers",
            collection.name()
        );
    }
}
