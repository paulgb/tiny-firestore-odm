use std::error::Error;
use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    TooFewParts(usize),
    WrongNumberOfParts(usize),
    InvalidPart(usize),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ParseError {}

pub enum ParentDocumentOrRoot {
    Root { project_id: String },
    ParentDocument { document: DocumentName },
}

impl ParentDocumentOrRoot {
    pub fn name(&self) -> String {
        match self {
            Self::Root { project_id } => {
                format!("projects/{}/databases/(default)/documents", project_id)
            }
            Self::ParentDocument { document } => document.name(),
        }
    }

    pub fn parent(&self) -> Option<CollectionName> {
        match self {
            Self::Root { .. } => None,
            Self::ParentDocument { document } => Some(document.collection.clone()),
        }
    }
}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct CollectionName {
    project_id: String,
    /// Vector of (collection, name) pairs of parent.
    parent_path: Vec<(String, String)>,

    /// Name of the "leaf" collection.
    collection: String,
}

impl CollectionName {
    pub fn new(project_id: &str, collection: &str) -> Self {
        CollectionName::new_with_path(project_id, &[], collection)
    }

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

    pub fn parent_collection(&self) -> Option<Self> {
        self.parent().parent()
    }

    pub fn document(&self, name: &str) -> DocumentName {
        DocumentName {
            collection: self.clone(),
            name: name.to_string(),
        }
    }

    pub fn leaf_name(&self) -> String {
        self.collection.clone()
    }

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

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct DocumentName {
    collection: CollectionName,
    name: String,
}

impl DocumentName {
    pub fn name(&self) -> String {
        format!("{}/{}", self.collection.name(), self.name)
    }

    pub fn parse(name: &str) -> Result<Self, ParseError> {
        let (collection_name, name) = name.rsplit_once("/").unwrap();

        let collection = CollectionName::parse(collection_name).unwrap();

        Ok(DocumentName {
            collection,
            name: name.to_string(),
        })
    }
}

pub trait QualifyDocumentName {
    fn qualify(&self, parent: &CollectionName) -> DocumentName;
}

impl QualifyDocumentName for &str {
    fn qualify(&self, parent: &CollectionName) -> DocumentName {
        parent.document(self)
    }
}

impl QualifyDocumentName for &DocumentName {
    fn qualify(&self, _parent: &CollectionName) -> DocumentName {
        (*self).clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
