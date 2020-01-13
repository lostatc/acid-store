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

    /// This repository format is not supported by this version of the library.
    #[error("This repository format is not supported by this version of the library.")]
    UnsupportedVersion,

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

    /// An I/O error occurred.
    #[error("{0}")]
    Io(#[from] io::Error),

    #[doc(hidden)]
    #[error("")]
    __NonExhaustive,
}

/// The result type for operations with a repository.
pub type Result<T> = result::Result<T, Error>;
