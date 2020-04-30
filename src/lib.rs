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

//! `acid-store` is a library for secure, deduplicated, transactional, and verifiable data storage.
//!
//! This crate provides high-level abstractions for data storage over a number of storage backends.
//!
//! This library currently provides the following abstractions for data storage:
//! - `ObjectRepository` is an object store which maps keys to seekable binary blobs.
//! - `FileRepository` is a virtual file system which can import and export files to the local OS
//! file system.
//! - `ValueRepository` is a persistent, heterogeneous, map-like collection.
//! - `VersionRepository` is an object store with support for content versioning.
//! - `ContentRepository` is a content-addressable storage which allows for accessing data by its
//! cryptographic hash.
//!
//! A repository stores its data in a `DataStore`, which is a small trait that can be implemented to
//! create new storage backends. The following data stores are provided out of the box:
//! - `DirectoryStore` stores data in a directory in the local file system.
//! - `SqliteStore` stores data in a SQLite database.
//! - `RedisStore` stores data on a Redis server.
//! - `S3Store` stores data in an Amazon S3 bucket.
//! - `RcloneStore` stores data in a varity of cloud storage backends using
//! [rclone](https://rclone.org/).
//! - `MemoryStore` stores data in memory.
//!
//! # Examples
//! ```
//! use std::io::{Read, Seek, Write, SeekFrom};
//! use acid_store::store::{MemoryStore, OpenStore, OpenOption};
//! use acid_store::repo::{OpenRepo, ObjectRepository, RepositoryConfig};
//!
//! fn main() -> acid_store::Result<()> {
//!     // Create a repository with the default configuration that stores data in memory.
//!     let mut repository = ObjectRepository::new_repo(
//!         MemoryStore::new(),
//!         RepositoryConfig::default(),
//!         None
//!     )?;
//!
//!     // Insert a key into the repository and get an object which can be used to read/write data.
//!     let mut object = repository.insert(String::from("Key"));
//!
//!     // Write data to the repository via `std::io::Write`.
//!     object.write_all(b"Data")?;
//!     object.flush();
//!
//!     // Get the object associated with a key.
//!     drop(object);
//!     let mut object = repository.get("Key").unwrap();
//!
//!     // Read data from the repository via `std::io::Read`.
//!     let mut data = Vec::new();
//!     object.read_to_end(&mut data)?;
//!
//!     assert_eq!(data, b"Data");
//!
//!     // Commit changes to the repository.
//!     drop(object);
//!     repository.commit()?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Features
//! Some functionality is gated behind cargo features:
//!
//! Types | Cargo Feature | Default
//! --- | --- | ---
//! `Encryption::XChaCha20Poly1305` | `encryption` | No
//! `Compression::Deflate`, `Compression::Lzma`, `Compression::Lz4` | `compression` | No
//! `CommonMetadata`, `UnixMetadata` | `file-metadata` | No
//! `DirectoryStore` | `store-directory` | Yes
//! `SqliteStore` | `store-sqlite` | No
//! `RedisStore` | `store-redis` | No
//! `S3Store` | `store-s3` | No
//! `RcloneStore` | `store-rclone` | No
//!
//! To use a feature which is not enabled by default, you must enable it in your `Cargo.toml`.

#![allow(dead_code)]

pub use uuid;

pub use error::{Error, Result};

mod error;
pub mod repo;
pub mod store;
