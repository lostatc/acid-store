/*
 * Copyright 2019 Garrett Powell
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use std::io;
use std::result;

use thiserror::Error as DeriveError;

/// The error type for operations with a repository.
///
/// This type can be converted `From` and `Into` an `io::Error` for compatibility with types from
/// `std::io` like `Read`, `Write`, and `Seek`. Even if the payload of the `io::Error` cannot be
/// downcast to a value of this type, it will be converted to `Error::Io`.
#[derive(Debug, DeriveError)]
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

    /// The repository is locked.
    #[error("The repository is locked.")]
    Locked,

    /// The repository is corrupt.
    #[error("The repository is corrupt.")]
    Corrupt,

    /// This format is not supported by this version of the library.
    #[error("This format is not supported by this version of the library.")]
    UnsupportedFormat,

    /// The provided key type does not match the data in the repository.
    #[error("The provided key type does not match the data in the repository.")]
    KeyType,

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
    ///
    /// This wraps the `DataStore::Error` provided by the data store.
    #[error("{0}")]
    Store(#[from] anyhow::Error),

    #[doc(hidden)]
    #[error("")]
    __NonExhaustive,
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
