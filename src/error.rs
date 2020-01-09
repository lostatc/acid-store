/*
 * Copyright 2019 Wren Powell
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
#[derive(Debug, DeriveError)]
pub enum Error {
    /// The repository already exists.
    #[error("The repository already exists.")]
    RepositoryAlreadyExists,

    /// The repository was not found.
    #[error("The repository was not found.")]
    RepositoryNotFound,

    /// The provided password was invalid.
    #[error("The provided password was invalid.")]
    Password,

    /// The repository is locked.
    #[error("The repository is locked.")]
    Locked,

    /// The repository is corrupt.
    #[error("The repository is corrupt.")]
    Corrupt,

    /// Some operation is not supported.
    #[error("This operation is not supported.")]
    Unsupported,

    /// The provided key type does not match the data in the repository.
    #[error("The provided key type does not match the data in the repository.")]
    KeyType,

    /// The provided entry path is invalid.
    #[error("The provided entry path is invalid.")]
    InvalidPath,

    /// An I/O error occurred.
    #[error("{0}")]
    Io(#[from] io::Error),

    #[doc(hidden)]
    #[error("")]
    __NonExhaustive,
}

/// The result type for operations with a repository.
pub type Result<T> = result::Result<T, Error>;
