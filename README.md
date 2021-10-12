# `tiny-firestore-odm`

`tiny-firestore-odm` is a lightweight Object Document Mapper for Firestore. It's a lightweight
layer on top of [`firestore-serde`](https://github.com/paulgb/firestore-serde), which does the
document/object translation, adding a Rust representation of Firestore "collections" along with
methods to create/modify/delete them.

The intent is not to provide access to all of Firestore's functionality, but to provide a
simplified interface centered around using Firestore as a key/value store for arbitrary
collections of (serializable) Rust objects.