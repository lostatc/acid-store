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

#![allow(dead_code)]
#![cfg(all(feature = "encryption", feature = "compression"))]

use std::fmt::Debug;
use std::hash::Hash;
#[cfg(any(feature = "store-directory", feature = "store-sqlite"))]
use std::path::Path;

use lazy_static::lazy_static;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};

use acid_store::repo::{Chunking, Compression, Encryption, Packing, RepoConfig};
use acid_store::store::{DataStore, MemoryConfig, MemoryStore, OpenStore};
#[cfg(feature = "store-directory")]
use acid_store::store::{DirectoryConfig, DirectoryStore};
#[cfg(feature = "store-sftp")]
use acid_store::store::{RcloneConfig, RcloneStore};
#[cfg(feature = "store-redis")]
use acid_store::store::{RedisConfig, RedisStore};
#[cfg(feature = "store-s3")]
use acid_store::store::{S3Config, S3Credentials, S3Region, S3Store};
#[cfg(feature = "store-sqlite")]
use acid_store::store::{SqliteConfig, SqliteStore};
#[cfg(feature = "store-sftp")]
use {
    acid_store::store::{SftpAuth, SftpConfig, SftpStore},
    std::path::PathBuf,
};

/// The minimum size of test data buffers.
pub const MIN_BUFFER_SIZE: usize = 2048;

/// The maximum size of test data buffers.
pub const MAX_BUFFER_SIZE: usize = 4096;

lazy_static! {
    /// The repository config used for testing fixed-size chunking.
    pub static ref FIXED_CONFIG: RepoConfig = {
        let mut config = RepoConfig::default();
        config.chunking = Chunking::Fixed { size: 256 };
        config.packing = Packing::None;
        config.encryption = Encryption::None;
        config.compression = Compression::None;
        config
    };

    /// The repository config used for testing encryption and compression.
    pub static ref ENCODING_CONFIG: RepoConfig = {
        let mut config = FIXED_CONFIG.to_owned();
        config.encryption = Encryption::XChaCha20Poly1305;
        config.compression = Compression::Lz4 { level: 1 };
        config
    };

    /// The repository config used for testing ZPAQ chunking.
    pub static ref ZPAQ_CONFIG: RepoConfig = {
        let mut config = FIXED_CONFIG.to_owned();
        config.chunking = Chunking::Zpaq { bits: 8 };
        config
    };

    /// The repository config used for testing packing with a size smaller than the chunk size.
    pub static ref FIXED_PACKING_SMALL_CONFIG: RepoConfig = {
        let mut config = FIXED_CONFIG.to_owned();
        // Smaller than the chunk size and not a factor of it.
        config.packing = Packing::Fixed(100);
        config
    };

    /// The repository config used for testing packing with a size larger than the chunk size.
    pub static ref FIXED_PACKING_LARGE_CONFIG: RepoConfig = {
        let mut config = FIXED_CONFIG.to_owned();
        // Larger than the chunk size and not a multiple of it.
        config.packing = Packing::Fixed(300);
        config
    };

    /// The repository config used for testing packing with ZPAQ chunking.
    pub static ref ZPAQ_PACKING_CONFIG: RepoConfig = {
        let mut config = ZPAQ_CONFIG.to_owned();
        config.packing = Packing::Fixed(256);
        config
    };
}

/// Remove all blocks in the given `store`.
pub fn truncate_store(store: &mut impl DataStore) -> anyhow::Result<()> {
    for block_id in store.list_blocks()? {
        store.remove_block(block_id)?;
    }
    Ok(())
}

pub fn memory_store() -> anyhow::Result<MemoryStore> {
    Ok(MemoryConfig::new().open()?)
}

#[cfg(feature = "store-directory")]
pub fn directory_store(directory: &Path) -> anyhow::Result<DirectoryStore> {
    let config = DirectoryConfig {
        path: directory.join("store"),
    };
    let mut store = config.open()?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-sqlite")]
pub fn sqlite_store(directory: &Path) -> anyhow::Result<SqliteStore> {
    let config = SqliteConfig {
        path: directory.join("store.db"),
    };
    let mut store = config.open()?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-redis")]
pub fn redis_store() -> anyhow::Result<RedisStore> {
    let url = dotenv::var("REDIS_URL").unwrap();
    let config = RedisConfig::from_url(&url).unwrap();
    let mut store = config.open()?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-s3")]
pub fn s3_store() -> anyhow::Result<S3Store> {
    let config = S3Config {
        bucket: dotenv::var("S3_BUCKET").unwrap(),
        region: S3Region::from_name(&dotenv::var("S3_REGION").unwrap()).unwrap(),
        credentials: S3Credentials::Basic {
            access_key: dotenv::var("S3_ACCESS_KEY").unwrap(),
            secret_key: dotenv::var("S3_SECRET_KEY").unwrap(),
        },
        prefix: String::from("test"),
    };
    let mut store = config.open()?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-sftp")]
pub fn sftp_store() -> anyhow::Result<SftpStore> {
    let sftp_server: String = dotenv::var("SFTP_SERVER").unwrap();
    let sftp_path: String = dotenv::var("SFTP_PATH").unwrap();
    let sftp_username: String = dotenv::var("SFTP_USERNAME").unwrap();
    let sftp_password: String = dotenv::var("SFTP_PASSWORD").unwrap();

    let config = SftpConfig {
        addr: sftp_server.parse().unwrap(),
        auth: SftpAuth::Password {
            username: sftp_username,
            password: sftp_password,
        },
        path: PathBuf::from(sftp_path),
    };

    let mut store = config.open()?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-rclone")]
pub fn rclone_store() -> anyhow::Result<RcloneStore> {
    let config = RcloneConfig {
        config: dotenv::var("RCLONE_REMOTE").unwrap(),
    };
    let mut store = config.open()?;
    truncate_store(&mut store)?;
    Ok(store)
}

/// Assert that two collections contain all the same elements, regardless of order.
pub fn assert_contains_all<T: Hash + Eq + Debug>(
    actual: impl IntoIterator<Item = T>,
    expected: impl IntoIterator<Item = T>,
) {
    assert_eq!(
        actual.into_iter().collect::<std::collections::HashSet<_>>(),
        expected
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
    )
}

/// Return a buffer containing `size` random bytes for testing purposes.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

/// Generate a random buffer of bytes of a random size.
pub fn random_buffer() -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    random_bytes(rng.gen_range(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE))
}
