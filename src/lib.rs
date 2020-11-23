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
//! This library currently provides the following abstractions for data storage. They can be found
//! in the `acid_store::repo` module.
//! - `KeyRepo` is an object store which maps keys to seekable binary blobs.
//! - `FileRepo` is a virtual file system which supports file metadata, special files, and
//! importing and exporting files to the local OS file system.
//! - `ValueRepo` is a persistent, heterogeneous, map-like collection.
//! - `VersionRepo` is an object store with support for content versioning.
//! - `ContentRepo` is a content-addressable storage which allows for accessing data by its
//! cryptographic hash.
//! - `ObjectRepo` is a low-level repository type which provides more direct access to the
//! underlying storage.
//!
//! A repository stores its data in a `DataStore`, which is a small trait that can be implemented to
//! create new storage backends. The following data stores are provided out of the box. They can be
//! found in the `acid_store::store` module.
//! - `DirectoryStore` stores data in a directory in the local file system.
//! - `SqliteStore` stores data in a SQLite database.
//! - `RedisStore` stores data on a Redis server.
//! - `S3Store` stores data in an Amazon S3 bucket.
//! - `SftpStore` stores data on an SFTP server.
//! - `RcloneStore` stores data in a varity of cloud storage backends using
//! [rclone](https://rclone.org/).
//! - `MemoryStore` stores data in memory.
//!
//! # Examples
//! ```
//! use std::io::{Read, Seek, Write, SeekFrom};
//! use acid_store::store::MemoryStore;
//! use acid_store::repo::{OpenOptions, key::KeyRepo};
//!
//! fn main() -> acid_store::Result<()> {
//!     // Create a `KeyRepo` with the default configuration that stores data in memory.
//!     let mut repository = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
//!
//!     // Insert a key into the repository and get an object which can be used to read/write data.
//!     let mut object = repository.insert(String::from("Key"));
//!
//!     // Write data to the repository via `std::io::Write`.
//!     object.write_all(b"Data")?;
//!     object.flush();
//!     drop(object);
//!
//!     // Get the object associated with a key.
//!     let mut object = repository.object("Key").unwrap();
//!
//!     // Read data from the repository via `std::io::Read`.
//!     let mut data = Vec::new();
//!     object.read_to_end(&mut data)?;
//!     drop(object);
//!
//!     assert_eq!(data, b"Data");
//!
//!     // Commit changes to the repository.
//!     repository.commit()?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Features
//! Some functionality is gated behind cargo features.
//!
//! Types | Cargo Feature | Default
//! --- | --- | ---
//! All `Encryption` variants except `Encryption::None` | `encryption` | No
//! All `Compression` variants except `Compression::None` | `compression` | No
//! `CommonMetadata`, `UnixMetadata`, `AccessQualifier`, `UnixSpecialType` | `file-metadata` | No
//! All `HashAlgorithm` variants except `HashAlgorithm::Blake3` | `hash-algorithms` | No
//! `DirectoryStore` | `store-directory` | Yes
//! `SqliteStore` | `store-sqlite` | No
//! `RedisStore` | `store-redis` | No
//! `S3Store` | `store-s3` | No
//! `SftpStore` | `store-sftp`| No
//! `RcloneStore` | `store-rclone` | No
//!
//! To use a feature which is not enabled by default, you must enable it in your `Cargo.toml`.

#![allow(dead_code)]

pub use anyhow;
pub use uuid;

pub use error::{Error, Result};

mod error;
pub mod repo;
pub mod store;
