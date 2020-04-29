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

//! Low-level backends for data storage.
//!
//! This module provides low-level data storage backends called data stores. A data store provides
//! only the most basic storage operations, and doesn't have to worry about providing features like
//! encryption, compression, deduplication, integrity checking, locking, or atomic transactions.
//! Those features are implemented at a higher level. Data stores are meant to be easy to implement
//! so that providing support for new storage backends is relatively painless.
//!
//! All data stores implement the `DataStore` trait. The `OpenStore` trait is meant to provide a
//! common interface for opening data stores, but implementing it is optional.
//!
//! See the crate-level documentation for a summary of the different data stores provided in this
//! module.
//!
//! This module additionally provides `MultiStore` which allows for storing multiple repositories in
//! a single data store.
//!
//! # Examples
//! Open a data store which stores data in a directory of the local file system. Create the data
//! store if it doesn't already exist, and truncate it if it does.
//! ```no_run
//! use acid_store::store::{DirectoryStore, OpenStore, OpenOption};
//!
//! let store = DirectoryStore::open(
//!     "/home/lostatc/store".into(),
//!     OpenOption::CREATE | OpenOption::TRUNCATE
//! ).unwrap();
//! ```

pub use multi::{MultiStore, ProxyStore};

pub use self::common::{DataStore, OpenOption, OpenStore};
#[cfg(feature = "store-directory")]
pub use self::directory::DirectoryStore;
pub use self::memory::MemoryStore;
#[cfg(all(unix, feature = "store-rclone"))]
pub use self::rclone::RcloneStore;
#[cfg(feature = "store-redis")]
pub use self::redis::RedisStore;
#[cfg(feature = "store-s3")]
pub use self::s3::S3Store;
#[cfg(feature = "store-sqlite")]
pub use self::sqlite::SqliteStore;

mod common;
mod directory;
mod memory;
mod multi;
mod rclone;
mod redis;
mod s3;
mod sqlite;
