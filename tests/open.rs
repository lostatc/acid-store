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

#![cfg(all(feature = "encryption", feature = "compression"))]

use serial_test::serial;
use tempfile::tempdir;
use uuid::Uuid;

#[cfg(feature = "store-directory")]
use acid_store::store::DirectoryStore;
#[cfg(feature = "store-sqlite")]
use acid_store::store::SqliteStore;
use acid_store::store::{DataStore, OpenOption, OpenStore};
use common::random_buffer;
#[cfg(feature = "store-rclone")]
use {acid_store::store::RcloneStore, common::rclone_config};
#[cfg(feature = "store-redis")]
use {acid_store::store::RedisStore, common::redis_config};
#[cfg(feature = "store-s3")]
use {acid_store::store::S3Store, common::s3_config};
#[cfg(feature = "store-sftp")]
use {acid_store::store::SftpStore, common::sftp_config};

mod common;

// Some tests in this module use the `serial_test` crate to force them to run in sequence because
// they access a shared resource. However, that crate doesn't seem to support test functions which
// return a `Result`, so those tests return `()` and unwrap `Result`s instead.

#[test]
#[cfg(feature = "store-directory")]
fn directory_create_new_with_existing_store_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::CREATE_NEW)?;
    let result = DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::CREATE_NEW);

    assert!(matches!(result, Err(acid_store::Error::AlreadyExists)));
    Ok(())
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_create_new_with_existing_store_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    SqliteStore::open(temp_dir.as_ref().join("store.db"), OpenOption::CREATE_NEW)?;
    let result = SqliteStore::open(temp_dir.as_ref().join("store.db"), OpenOption::CREATE_NEW);

    assert!(matches!(result, Err(acid_store::Error::AlreadyExists)));
    Ok(())
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_create_new_with_existing_store_errs() {
    RedisStore::open(
        redis_config().unwrap(),
        OpenOption::CREATE | OpenOption::TRUNCATE,
    )
    .unwrap();
    let result = RedisStore::open(redis_config().unwrap(), OpenOption::CREATE_NEW);

    assert!(matches!(result, Err(acid_store::Error::AlreadyExists)));
}

#[test]
#[serial(s3)]
#[cfg(feature = "store-s3")]
fn s3_create_new_with_existing_store_errs() {
    S3Store::open(
        s3_config().unwrap(),
        OpenOption::CREATE | OpenOption::TRUNCATE,
    )
    .unwrap();
    let result = S3Store::open(s3_config().unwrap(), OpenOption::CREATE_NEW);

    assert!(matches!(result, Err(acid_store::Error::AlreadyExists)));
}

#[test]
#[serial(sftp)]
#[cfg(feature = "store-sftp")]
fn sftp_create_new_with_existing_store_errs() {
    SftpStore::open(
        sftp_config().unwrap(),
        OpenOption::CREATE | OpenOption::TRUNCATE,
    )
    .unwrap();
    let result = SftpStore::open(sftp_config().unwrap(), OpenOption::CREATE_NEW);

    assert!(matches!(result, Err(acid_store::Error::AlreadyExists)));
}

#[test]
#[serial(rclone)]
#[cfg(feature = "store-rclone")]
fn rclone_create_new_with_existing_store_errs() {
    RcloneStore::open(rclone_config(), OpenOption::CREATE | OpenOption::TRUNCATE).unwrap();
    let result = RcloneStore::open(rclone_config(), OpenOption::CREATE_NEW);

    assert!(matches!(result, Err(acid_store::Error::AlreadyExists)));
}

fn truncate_store<S: OpenStore + DataStore, F: Fn() -> S::Config>(config: F) -> anyhow::Result<()> {
    let mut store = S::open(config(), OpenOption::CREATE | OpenOption::TRUNCATE)?;
    store.write_block(Uuid::new_v4(), &random_buffer())?;

    assert!(!store.list_blocks()?.is_empty());

    let mut store = S::open(config(), OpenOption::TRUNCATE)?;

    assert!(store.list_blocks()?.is_empty());

    Ok(())
}

#[test]
#[cfg(feature = "store-directory")]
fn directory_truncate_store() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    truncate_store::<DirectoryStore, _>(|| temp_dir.as_ref().join("store"))
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_truncate_store() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    truncate_store::<SqliteStore, _>(|| temp_dir.as_ref().join("store.db"))
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_truncate_store() {
    truncate_store::<RedisStore, _>(|| redis_config().unwrap()).unwrap();
}

#[test]
#[serial(s3)]
#[cfg(feature = "store-s3")]
fn s3_truncate_store() {
    truncate_store::<S3Store, _>(|| s3_config().unwrap()).unwrap();
}

#[test]
#[serial(sftp)]
#[cfg(feature = "store-sftp")]
fn sftp_truncate_store() {
    truncate_store::<SftpStore, _>(|| sftp_config().unwrap()).unwrap();
}

#[test]
#[serial(rclone)]
#[cfg(feature = "store-rclone")]
fn rclone_truncate_store() {
    truncate_store::<RcloneStore, _>(|| rclone_config()).unwrap();
}
