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

#[cfg(any(feature = "store-directory", feature = "store-sqlite"))]
use std::path::Path;

use rstest::*;
use tempfile::TempDir;

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
use std::ops::{Deref, DerefMut};
#[cfg(feature = "store-sftp")]
use {
    acid_store::store::{SftpAuth, SftpConfig, SftpStore},
    std::path::PathBuf,
};

/// Remove all blocks in the given `store`.
fn truncate_store(store: &mut impl DataStore) -> anyhow::Result<()> {
    for block_id in store.list_blocks()? {
        store.remove_block(block_id)?;
    }
    Ok(())
}

/// A value which is tied to the lifetime of a temporary directory.
pub struct WithTempDir<T> {
    directory: TempDir,
    value: T,
}

impl<T> Deref for WithTempDir<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for WithTempDir<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[fixture]
pub fn memory_store() -> MemoryStore {
    MemoryConfig::new().open().unwrap()
}

#[cfg(feature = "store-directory")]
#[fixture]
pub fn directory_store() -> WithTempDir<DirectoryStore> {
    let directory = tempfile::tempdir().unwrap();
    let config = DirectoryConfig {
        path: directory.as_ref().join("store"),
    };
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    WithTempDir {
        directory,
        value: store,
    }
}

#[cfg(feature = "store-sqlite")]
#[fixture]
pub fn sqlite_store() -> WithTempDir<SqliteStore> {
    let directory = tempfile::tempdir().unwrap();
    let config = SqliteConfig {
        path: directory.as_ref().join("store.db"),
    };
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    WithTempDir {
        directory,
        value: store,
    }
}

#[cfg(feature = "store-redis")]
#[fixture]
pub fn redis_store() -> RedisStore {
    let url = dotenv::var("REDIS_URL").unwrap();
    let config = RedisConfig::from_url(&url).unwrap();
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    store
}

#[cfg(feature = "store-s3")]
#[fixture]
pub fn s3_store() -> S3Store {
    let config = S3Config {
        bucket: dotenv::var("S3_BUCKET").unwrap(),
        region: S3Region::from_name(&dotenv::var("S3_REGION").unwrap()).unwrap(),
        credentials: S3Credentials::Basic {
            access_key: dotenv::var("S3_ACCESS_KEY").unwrap(),
            secret_key: dotenv::var("S3_SECRET_KEY").unwrap(),
        },
        prefix: String::from("test"),
    };
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    store
}

#[cfg(feature = "store-sftp")]
#[fixture]
pub fn sftp_store() -> SftpStore {
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

    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    store
}

#[cfg(feature = "store-rclone")]
#[fixture]
pub fn rclone_store() -> RcloneStore {
    let config = RcloneConfig {
        config: dotenv::var("RCLONE_REMOTE").unwrap(),
    };
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    store
}
