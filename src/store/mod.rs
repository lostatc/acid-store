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
//! All data stores implement the `DataStore` trait.
//!
//! Many of the data stores in this module are gated behind cargo features. See the crate-level
//! documentation for more details.

pub use self::common::DataStore;
#[cfg(feature = "store-directory")]
pub use self::directory_store::DirectoryStore;
pub use self::memory_store::MemoryStore;
#[cfg(all(unix, feature = "store-rclone"))]
pub use self::rclone_store::RcloneStore;
#[cfg(feature = "store-redis")]
pub use {self::redis_store::RedisStore, redis};
#[cfg(feature = "store-s3")]
pub use {self::s3_store::S3Store, s3};
#[cfg(feature = "store-sftp")]
pub use {self::sftp_store::SftpStore, ssh2};
#[cfg(feature = "store-sqlite")]
pub use {self::sqlite_store::SqliteStore, rusqlite};

mod common;
mod directory_store;
mod memory_store;
mod rclone_store;
mod redis_store;
mod s3_store;
mod sftp_store;
mod sqlite_store;
