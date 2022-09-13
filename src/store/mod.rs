//! Low-level backends for data storage.
//!
//! This module provides low-level data storage backends called data stores. A data store provides
//! only the most basic storage operations, and doesn't have to worry about providing features like
//! encryption, compression, deduplication, integrity checking, or atomic transactions. Those
//! features are implemented at a higher level. Data stores are meant to be easy to implement so
//! that providing support for new storage backends is relatively painless.
//!
//! All data stores implement the [`DataStore`] trait.
//!
//! For each data store, there is a corresponding type which provides the necessary configuration to
//! open that data store. These config types implement [`OpenStore`]. Typically, you'll use these
//! config types with [`OpenOptions`] to open repositories. You'll almost never need to use the
//! [`OpenStore`] or [`DataStore`] traits directly.
//!
//! [`DataStore`]: crate::store::DataStore
//! [`OpenStore`]: crate::store::OpenStore
//! [`OpenOptions`]: crate::repo::OpenOptions

pub use self::data_store::{BlockId, BlockKey, BlockType, DataStore};
#[cfg(feature = "store-directory")]
pub use self::directory_store::{DirectoryConfig, DirectoryStore};
pub use self::memory_store::{MemoryConfig, MemoryStore};
pub use self::open_store::OpenStore;
#[cfg(feature = "store-rclone")]
pub use self::rclone_store::{RcloneConfig, RcloneStore};
#[cfg(feature = "store-redis")]
pub use self::redis_store::{RedisAddr, RedisConfig, RedisStore};
#[cfg(feature = "store-s3")]
pub use self::s3_store::{S3Config, S3Credentials, S3Region, S3Store};
#[cfg(feature = "store-sftp")]
pub use self::sftp_store::{SftpAuth, SftpConfig, SftpStore};
#[cfg(feature = "store-sqlite")]
pub use self::sqlite_store::{SqliteConfig, SqliteStore};

mod data_store;
mod directory_store;
mod memory_store;
mod open_store;
mod rclone_store;
mod redis_store;
mod s3_store;
mod sftp_store;
mod sqlite_store;
