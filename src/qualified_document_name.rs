use std::error::Error;
use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum ParseError {
    WrongNumberOfParts(usize),
    InvalidPart(usize),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ParseError {}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct QualifiedCollectionName {
    project_id: String,
    /// Vector of (collection, name) pairs of parent.
    parent_parts: Vec<(String, String)>,
    collection: String,
}

impl QualifiedCollectionName {
    pub fn new(project_id: &str, collection: &str) -> Self {
        QualifiedCollectionName {
            project_id: project_id.to_string(),
            collection: collection.to_string(),
            parent_parts: Vec::new(),
        }
    }

    pub fn document(&self, name: &str) -> QualifiedDocumentName {
        QualifiedDocumentName {
            collection: self.clone(),
            name: name.to_string(),
        }
    }

    pub fn name(&self) -> String {
        format!(
            "projects/{}/databases/(default)/documents/{}",
            self.project_id, self.collection
        )
    }

    pub fn parent_name(&self) -> String {
        format!("projects/{}/databases/(default)/documents", self.project_id)
    }

    pub fn parse(name: &str) -> Result<Self, ParseError> {
        let parts: Vec<&str> = name.split("/").into_iter().collect();

        if parts.len() != 6 {
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
        let collection = parts.get(5).unwrap().to_string();

        Ok(QualifiedCollectionName {
            project_id,
            collection,
            parent_parts: Vec::new(),
        })
    }
}

#[derive(Clone, Hash, Debug, PartialEq, Eq)]
pub struct QualifiedDocumentName {
    collection: QualifiedCollectionName,
    name: String,
}

impl QualifiedDocumentName {
    pub fn name(&self) -> String {
        format!("{}/{}", self.collection.name(), self.name)
    }

    pub fn parse(name: &str) -> Result<Self, ParseError> {
        let (collection_name, name) = name.rsplit_once("/").unwrap();

        let collection = QualifiedCollectionName::parse(collection_name).unwrap();

        Ok(QualifiedDocumentName {
            collection,
            name: name.to_string(),
        })
    }
}

pub trait QualifyDocumentName {
    fn qualify(&self, path: &str) -> QualifiedDocumentName;
}

impl QualifyDocumentName for &str {
    fn qualify(&self, path: &str) -> QualifiedDocumentName {
        todo!()
    }
}

impl QualifyDocumentName for &QualifiedDocumentName {
    fn qualify(&self, _path: &str) -> QualifiedDocumentName {
        (*self).clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_construct_document_name() {
        let collection = QualifiedCollectionName::new("my-project", "things");

        assert_eq!(
            "projects/my-project/databases/(default)/documents/things",
            collection.name()
        );

        assert_eq!(
            "projects/my-project/databases/(default)/documents",
            collection.parent_name()
        );

        let doc1 = collection.document("thing1");

        assert_eq!(
            "projects/my-project/databases/(default)/documents/things/thing1",
            doc1.name()
        );
    }

    #[test]
    fn test_parse_collection_name() {
        let name_to_parse = "projects/employee-directory/databases/(default)/documents/people";

        let expected = QualifiedCollectionName {
            collection: "people".to_string(),
            parent_parts: vec![],
            project_id: "employee-directory".to_string(),
        };

        let result = QualifiedCollectionName::parse(name_to_parse).unwrap();

        assert_eq!(expected, result);
    }

    #[test]
    fn test_fail_parse_collection_name() {
        assert_eq!(
            ParseError::WrongNumberOfParts(2),
            QualifiedCollectionName::parse("projects/employee-directory").unwrap_err()
        );

        assert_eq!(
            ParseError::WrongNumberOfParts(7),
            QualifiedCollectionName::parse("projects/employee-directory/databases/(default)/documents/people/stuff").unwrap_err()
        );

        assert_eq!(
            ParseError::InvalidPart(0),
            QualifiedCollectionName::parse("project/employee-directory/databases/(default)/documents/people").unwrap_err()
        );

        assert_eq!(
            ParseError::InvalidPart(2),
            QualifiedCollectionName::parse("projects/employee-directory/databa/(default)/documents/people").unwrap_err()
        );

        assert_eq!(
            ParseError::InvalidPart(3),
            QualifiedCollectionName::parse("projects/employee-directory/databases/default/documents/people").unwrap_err()
        );

        assert_eq!(
            ParseError::InvalidPart(4),
            QualifiedCollectionName::parse("projects/employee-directory/databases/(default)/stuff/people").unwrap_err()
        );
    }

    #[test]
    fn test_parse_document_name() {
        let name_to_parse = "projects/employee-directory/databases/(default)/documents/people/jack";

        let expected = QualifiedDocumentName {
            collection: QualifiedCollectionName {
                collection: "people".to_string(),
                parent_parts: vec![],
                project_id: "employee-directory".to_string(),
            },
            name: "jack".to_string(),
        };

        let result = QualifiedDocumentName::parse(name_to_parse).unwrap();

        assert_eq!(expected, result);
    }
}
