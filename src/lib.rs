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
//! in the [`crate::repo`] module.
//! - [`KeyRepo`] is an object store which maps keys to seekable binary blobs.
//! - [`FileRepo`] is a virtual file system which supports file metadata, special files, and
//! importing and exporting files to the local OS file system.
//! - [`ValueRepo`] is a persistent, heterogeneous, map-like collection.
//! - [`VersionRepo`] is an object store with support for content versioning.
//! - [`ContentRepo`] is a content-addressable storage which allows for accessing data by its
//! cryptographic hash.
//! - [`ObjectRepo`] is a low-level repository type which provides more direct access to the
//! underlying storage.
//!
//! A repository stores its data in a [`DataStore`], which is a small trait that can be implemented
//! to create new storage backends. The following data stores are provided out of the box. They can
//! be found in the [`crate::store`] module.
//! - [`DirectoryStore`] stores data in a directory in the local file system.
//! - [`SqliteStore`] stores data in a SQLite database.
//! - [`RedisStore`] stores data on a Redis server.
//! - [`S3Store`] stores data in an Amazon S3 bucket.
//! - [`SftpStore`] stores data on an SFTP server.
//! - [`RcloneStore`] stores data in a varity of cloud storage backends using
//! [rclone].
//! - [`MemoryStore`] stores data in memory.
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
//! Feature | Description | Types | Default
//! --- | --- | --- | ---
//! `encryption` | Encrypt repositories | All [`Encryption`] variants except [`Encryption::None`] | No
//! `compression` | Compress repositories | All [`Compression`] variants except [`Compression::None`] | No
//! `file-metadata` | Store file metadata and special file types in [`FileRepo`] | [`CommonMetadata`], [`UnixMetadata`], [`AccessQualifier`], [`UnixSpecialType`] | No
//! `hash-algorithms` | Use hash algorithms other than BLAKE3 in [`ContentRepo`] | All [`HashAlgorithm`] variants except [`HashAlgorithm::Blake3`] | No
//! `store-directory` | Store data in a directory in the local file system | [`DirectoryStore`] | Yes
//! `store-sqlite` | Store data in a SQLite database | [`SqliteStore`] | No
//! `store-redis` | Store data on a Redis server | [`RedisStore`], [`RedisConfig`], [`RedisAddr`] | No
//! `store-s3` | Store data in an Amazon S3 bucket | [`S3Store`], [`S3Config`], [`S3Credentials`], [`S3Region`] | No
//! `store-sftp` | Store data on an SFTP server | [`SftpStore`], [`SftpConfig`], [`SftpAuth`] | No
//! `store-rclone` | Store data in cloud storage via [rclone] | [`RcloneStore`] | No
//!
//! To use a feature which is not enabled by default, you must enable it in your `Cargo.toml`.
//!
//! [rclone]: https://rclone.org/
//!
//! [`KeyRepo`]: crate::repo::key::KeyRepo
//! [`FileRepo`]: crate::repo::file::FileRepo
//! [`ValueRepo`]: crate::repo::value::ValueRepo
//! [`VersionRepo`]: crate::repo::version::VersionRepo
//! [`ContentRepo`]: crate::repo::content::ContentRepo
//! [`ObjectRepo`]: crate::repo::object::ObjectRepo
//!
//! [`DataStore`]: crate::store::DataStore
//! [`DirectoryStore`]: crate::store::DirectoryStore
//! [`SqliteStore`]: crate::store::SqliteStore
//! [`RedisStore`]: crate::store::RedisStore
//! [`S3Store`]: crate::store::S3Store
//! [`SftpStore`]: crate::store::SftpStore
//! [`RcloneStore`]: crate::store::RcloneStore
//! [`MemoryStore`]: crate::store::MemoryStore
//!
//! [`Encryption`]: crate::repo::Encryption
//! [`Encryption::None`]: crate::repo::Encryption::None
//! [`Compression`]: crate::repo::Compression
//! [`Compression::None`]: crate::repo::Compression::None
//! [`CommonMetadata`]: crate::repo::file::CommonMetadata
//! [`UnixMetadata`]: crate::repo::file::UnixMetadata
//! [`AccessQualifier`]: crate::repo::file::AccessQualifier
//! [`UnixSpecialType`]: crate::repo::file::UnixSpecialType
//! [`HashAlgorithm`]: crate::repo::content::HashAlgorithm
//! [`HashAlgorithm::Blake3`]: crate::repo::content::HashAlgorithm::Blake3
//! [`RedisConfig`]: crate::store::RedisConfig
//! [`RedisAddr`]: crate::store::RedisAddr
//! [`S3Config`]: crate::store::S3Config
//! [`S3Credentials`]: crate::store::S3Credentials
//! [`S3Region`]: crate::store::S3Region
//! [`SftpConfig`]: crate::store::SftpConfig
//! [`SftpAuth`]: crate::store::SftpAuth

#![allow(dead_code)]

pub use anyhow;
pub use uuid;

pub use error::{Error, Result};

mod error;
pub mod repo;
pub mod store;
