use std::io;
use std::result;

use thiserror::Error as DeriveError;

use crate::store;

/// The error type for operations with a repository.
///
/// This type can be converted `From` and `Into` an `io::Error` for compatibility with types from
/// `std::io` like `Read`, `Write`, and `Seek`. Even if the payload of the `io::Error` cannot be
/// downcast to a value of this type, it will be converted to `Error::Io`.
#[derive(Debug, DeriveError)]
#[non_exhaustive]
pub enum Error {
    /// A resource already exists.
    #[error("A resource already exists.")]
    AlreadyExists,

    /// A resource was not found.
    #[error("A resource was not found.")]
    NotFound,

    /// The provided password was invalid.
    #[error("The provided password was invalid.")]
    Password,

    /// A resource is locked.
    #[error("A resource is locked.")]
    Locked,

    /// A resource is not locked.
    #[error("A resource is not locked.")]
    NotLocked,

    /// The repository is corrupt.
    #[error("The repository is corrupt.")]
    Corrupt,

    /// This data store is an unsupported format.
    #[error("This data store is an unsupported format.")]
    UnsupportedStore,

    /// This repository is an unsupported format.
    #[error("This repository is an unsupported format.")]
    UnsupportedRepo,

    /// The given savepoint is invalid.
    #[error("The given savepoint is invalid.")]
    InvalidSavepoint,

    /// This object is no longer valid.
    #[error("This object is no longer valid.")]
    InvalidObject,

    /// A transaction is currently in progress for this object.
    #[error("A transaction is currently in progress for this object.")]
    TransactionInProgress,

    /// This file type is not supported.
    #[error("This file type is not supported.")]
    FileType,

    /// The provided file path is invalid.
    #[error("The provided file path is invalid.")]
    InvalidPath,

    /// The directory is not empty.
    #[error("The directory is not empty.")]
    NotEmpty,

    /// The file is not a directory.
    #[error("The file is not a directory.")]
    NotDirectory,

    /// The file is not a regular file.
    #[error("The file is not a regular file.")]
    NotFile,

    /// A value could not be serialized.
    #[error("A value could not be serialized.")]
    Serialize,

    /// A value could not be deserialized.
    #[error("A value could not be deserialized.")]
    Deserialize,

    /// Ciphertext verification failed or data is otherwise invalid.
    #[error("Ciphertext verification failed or data is otherwise invalid.")]
    InvalidData,

    /// An I/O error occurred.
    #[error("{0}")]
    Io(io::Error),

    /// An error occurred with the data store.
    #[error("{0}")]
    Store(store::Error),
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        io::Error::new(io::ErrorKind::Other, error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        let kind = error.kind();
        match error.into_inner() {
            Some(payload) => match payload.downcast::<Error>() {
                Ok(crate_error) => *crate_error,
                Err(other_error) => Error::Io(io::Error::new(kind, other_error)),
            },
            None => Error::Io(io::Error::from(kind)),
        }
    }
}

/// The result type for operations with a repository.
pub type Result<T> = result::Result<T, Error>;