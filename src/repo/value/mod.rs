//! A persistent, heterogeneous, map-like collection.
//!
//! This module contains the [`ValueRepo`] repository type.
//!
//! This is a repository which maps keys to concrete values instead of binary blobs. Values are
//! serialized and deserialized automatically using a space-efficient binary format.
//!
//! Like other repositories, changes made to the repository are not persisted to the data store
//! until [`Commit::commit`] is called. For details about deduplication, compression, encryption,
//! and locking, see the module-level documentation for [`crate::repo`].
//!
//! [`ValueRepo`]: crate::repo::value::ValueRepo
//! [`Commit::commit`]: crate::repo::Commit::commit

pub use self::iter::Keys;
pub use self::repository::ValueRepo;

mod iter;
mod repository;
