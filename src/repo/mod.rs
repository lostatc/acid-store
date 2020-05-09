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
//! Repositories implement `OpenRepo`, which can be used to create a new repository or open an
//! existing one.
//!
//! `ObjectRepository` is the main repository type provided by this module. It's meant to be easily
//! extensible to fit most use-cases, and all other repository types are implemented on top of it.
//! The other repository types provided by this module can be found in sub-modules.

pub use object::{
    Compression, ContentId, Encryption, Key, LockStrategy, Object, ObjectRepository,
    ReadOnlyObject, RepositoryConfig, RepositoryInfo, RepositoryStats, ResourceLimit,
};
pub use open_repo::OpenRepo;

pub mod content;
pub mod file;
mod key_id;
mod object;
mod open_repo;
pub mod value;
pub mod version;
mod version_id;
