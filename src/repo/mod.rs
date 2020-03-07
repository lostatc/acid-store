/*
 * Copyright 2019-2020 Wren Powell
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

//! High-level abstractions for data storage.
//!
//! This module provides abstractions for data storage called repositories. Each repository is
//! backed by a `DataStore`, and provides features like encryption, compression, deduplication,
//! integrity checking, locking, and atomic transactions.
//!
//! See the crate-level documentation for a summary of the different types of repositories provided
//! in this module. `ObjectRepository` is meant to be easily extensible to fit most use-cases; all
//! the other repositories in this module are implemented using `ObjectRepository`.

pub use content::{ContentRepository, HashAlgorithm};
#[cfg(feature = "file-metadata")]
pub use file::CommonMetadata;
#[cfg(all(unix, feature = "file-metadata"))]
pub use file::UnixMetadata;
pub use file::{Entry, EntryPath, FileMetadata, FileRepository, FileType, NoMetadata};
pub use object::{
    Compression, ContentId, Encryption, Key, LockStrategy, Object, ObjectRepository,
    ReadOnlyObject, RepositoryConfig, RepositoryInfo, RepositoryStats, ResourceLimit,
};
pub use value::ValueRepository;
pub use version::{Version, VersionRepository};

mod content;
mod file;
mod object;
mod value;
mod version;
