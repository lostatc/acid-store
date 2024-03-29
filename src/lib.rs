#![cfg_attr(docsrs, feature(doc_cfg))]

//! acid-store is a library for secure, deduplicated, and transactional data storage. It provides
//! abstractions for data storage over a number of storage backends.
//!
//! This library provides the following abstractions for data storage, called repositories. They can
//! be found in the [`crate::repo`] module.
//!
//! - [`KeyRepo`][crate::repo::key] is an object store which maps keys to seekable binary blobs.
//! - [`ValueRepo`][crate::repo::value] is a persistent, heterogeneous, map-like collection.
//! - [`FileRepo`] is a virtual file system which can be mounted via FUSE and supports file
//! metadata, special files, sparse files, hard links, and importing and exporting files to the
//! local file system.
//! - [`StateRepo`] is a low-level repository type which can be used to implement higher-level
//! repository types.
//!
//! A repository stores its data in a [`DataStore`], which is a small trait that can be implemented
//! to create new storage backends. The following data stores are provided out of the box. They can
//! be found in the [`crate::store`] module.
//!
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
//!
//! ```
//! use std::io::{Read, Seek, Write, SeekFrom};
//! use acid_store::store::MemoryConfig;
//! use acid_store::repo::{OpenMode, OpenOptions, Commit, key::KeyRepo};
//!
//! fn main() -> acid_store::Result<()> {
//!     // Create a `KeyRepo` where objects are indexed by strings and data is stored in memory.
//!     let mut repo: KeyRepo<String> = OpenOptions::new()
//!         .mode(OpenMode::CreateNew)
//!         .open(&MemoryConfig::new())?;
//!
//!     // Insert a key into the repository and get an object which can be used to read/write data.
//!     let mut object = repo.insert(String::from("Key"));
//!
//!     // Write data to the repository via `std::io::Write` and commit changes to this object.
//!     write!(object, "Data")?;
//!     object.commit()?;
//!     drop(object);
//!
//!     // Get the object associated with a key.
//!     let mut object = repo.object("Key").unwrap();
//!
//!     // Read data from the object via `std::io::Read`.
//!     let mut data = Vec::new();
//!     object.read_to_end(&mut data)?;
//!     drop(object);
//!
//!     assert_eq!(data, b"Data");
//!
//!     // Commit changes to the repository.
//!     repo.commit()?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Features
//!
//! Some functionality is gated behind Cargo features. To use any of these features, you must enable
//! them in your `Cargo.toml`.
//!
//! These features enable different repository types.
//!
//! Feature        | Description
//! ---            | ---
//! `repo-value`   | Use the [`ValueRepo`] repository type
//! `repo-file`    | Use the [`FileRepo`] repository type
//!
//! These features enable different [`DataStore`] implementations.
//!
//! Feature           | Description
//! ---               | ---
//! `store-directory` | Store data in a directory in the local file system
//! `store-sqlite`    | Store data in a SQLite database
//! `store-redis`     | Store data on a Redis server
//! `store-s3`        | Store data in an Amazon S3 bucket
//! `store-sftp`      | Store data on an SFTP server
//! `store-rclone`    | Store data in cloud storage via [rclone]
//!
//! These features enable additional functionality.
//!
//! Feature           | Description
//! ---               | ---
//! `encryption`      | Encrypt repositories
//! `compression`     | Compress repositories
//! `file-metadata`   | Store file metadata and special file types in [`FileRepo`]
//! `fuse-mount`      | Mount a [`FileRepo`] as a FUSE file system
//!
//! These features have native dependencies. This table shows their package names on Ubuntu.
//!
//! Feature         | Build Dependencies           | Runtime Dependencies
//! ---             | ---                          | ---
//! `file-metadata` | `libacl1-dev`                | `acl`
//! `fuse-mount`    | `libfuse3-dev`, `pkg-config` | `fuse3`
//!
//! [rclone]: https://rclone.org/
//!
//! [`KeyRepo`]: crate::repo::key
//! [`FileRepo`]: crate::repo::file
//! [`ValueRepo`]: crate::repo::value
//! [`StateRepo`]: crate::repo::state
//!
//! [`DataStore`]: crate::store::DataStore
//! [`DirectoryStore`]: crate::store::DirectoryStore
//! [`SqliteStore`]: crate::store::SqliteStore
//! [`RedisStore`]: crate::store::RedisStore
//! [`S3Store`]: crate::store::S3Store
//! [`SftpStore`]: crate::store::SftpStore
//! [`RcloneStore`]: crate::store::RcloneStore
//! [`MemoryStore`]: crate::store::MemoryStore

#![forbid(unsafe_code)]

pub use uuid;

pub use error::{Error, Result};

mod error;
mod id;
pub mod repo;
pub mod store;
