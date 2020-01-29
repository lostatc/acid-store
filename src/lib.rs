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

//! `data-store` is a library for secure, deduplicated, transactional, and verifiable data storage.
//!
//! This crate provides high-level abstractions for data storage over a number of storage backends.
//! Storage backends are easy to implement, and this library builds on top of them to provide the
//! following features:
//! - Content-defined block-level deduplication
//! - Transparent encryption
//! - Transparent compression
//! - Integrity checking of data and metadata
//! - Atomicity, consistency, isolation, and durability (ACID)
//!
//! This library currently provides the following abstractions for data storage:
//! - `ObjectRepository` is an object store which maps keys to binary blobs.
//! - `FileRepository` is a file archive like ZIP or TAR which supports symbolic links, modification
//! times, POSIX permissions, and extended attributes.
//! - `ValueRepository` is a persistent, heterogeneous, map-like collection.
//! - `VersionRepository` is an object store with support for versioning.
//!
//! A repository stores its data in a `DataStore`, which is a small trait that can be implemented to
//! create new storage backends. The following data stores are provided out of the box:
//! - `DirectoryStore` stores data in a directory in the local file system.
//! - `SqliteStore` stores data in a SQLite database.
//! - `MemoryStore` stores data in memory.
//!
//! # Features
//! Some repositories and data stores are gated behind cargo features:
//!
//! Type | Cargo Feature
//! --- | ---
//! `FileRepository` | `repo-file`
//! `ValueRepository` | `repo-value`
//! `VersionRepository` | `repo-version`
//! `DirectoryStore` | `store-directory`
//! `SqliteStore` | `store-sqlite`
//!
//! To use one of these types, you must enable the corresponding feature in your `Cargo.toml`.

#![allow(dead_code)]

#[cfg(feature = "repo-file")]
pub use relative_path;
pub use uuid;

pub use env::init;
pub use error::{Error, Result};

mod env;
mod error;
pub mod repo;
pub mod store;
