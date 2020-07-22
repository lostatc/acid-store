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

#[cfg(any(
    feature = "store-s3",
    feature = "store-redis",
    feature = "store-rclone",
    feature = "store_sftp",
))]
use serial_test::serial;
use tempfile::tempdir;
use uuid::Uuid;

use acid_store::store::{DataStore, MemoryStore};
#[cfg(feature = "store-directory")]
use common::directory_store;
#[cfg(feature = "store-rclone")]
use common::rclone_store;
#[cfg(feature = "store-redis")]
use common::redis_store;
#[cfg(feature = "store-s3")]
use common::s3_store;
#[cfg(feature = "store-sftp")]
use common::sftp_store;
#[cfg(feature = "store-sqlite")]
use common::sqlite_store;
use common::{assert_contains_all, random_buffer};

mod common;

// Some tests in this module use the `serial_test` crate to force them to run in sequence because
// they access a shared resource. However, that crate doesn't seem to support test functions which
// return a `Result`, so those tests return `()` and unwrap `Result`s instead.

fn read_block(mut store: impl DataStore) -> anyhow::Result<()> {
    let id = Uuid::new_v4();

    assert_eq!(store.read_block(id)?, None);

    let expected_block = random_buffer();
    store.write_block(id, expected_block.as_slice())?;

    assert_eq!(store.read_block(id)?, Some(expected_block));

    Ok(())
}

#[test]
fn memory_read_block() -> anyhow::Result<()> {
    read_block(MemoryStore::new())
}

#[test]
#[cfg(feature = "store-directory")]
fn directory_read_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let store = directory_store(temp_dir.as_ref())?;
    read_block(store)
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_read_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let store = sqlite_store(temp_dir.as_ref())?;
    read_block(store)
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_read_block() {
    let store = redis_store().unwrap();
    read_block(store).unwrap();
}

#[test]
#[serial(s3)]
#[cfg(feature = "store-s3")]
fn s3_read_block() {
    let store = s3_store().unwrap();
    read_block(store).unwrap();
}

#[test]
#[serial(sftp)]
#[cfg(feature = "store-sftp")]
fn sftp_read_block() {
    let store = sftp_store().unwrap();
    read_block(store).unwrap();
}

#[test]
#[serial(rclone)]
#[cfg(feature = "store-rclone")]
fn rclone_read_block() {
    let store = rclone_store().unwrap();
    read_block(store).unwrap();
}

fn overwrite_block(mut store: impl DataStore) -> anyhow::Result<()> {
    let id = Uuid::new_v4();
    let expected_block = random_buffer();

    store.write_block(id, random_buffer().as_slice())?;
    store.write_block(id, expected_block.as_slice())?;

    assert_eq!(store.read_block(id)?, Some(expected_block));

    Ok(())
}

#[test]
fn memory_overwrite_block() -> anyhow::Result<()> {
    overwrite_block(MemoryStore::new())
}

#[test]
#[cfg(feature = "store-directory")]
fn directory_overwrite_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let store = directory_store(temp_dir.as_ref())?;
    overwrite_block(store)
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_overwrite_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let store = sqlite_store(temp_dir.as_ref())?;
    overwrite_block(store)
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_overwrite_block() {
    let store = redis_store().unwrap();
    overwrite_block(store).unwrap();
}

#[test]
#[serial(s3)]
#[cfg(feature = "store-s3")]
fn s3_overwrite_block() {
    let store = s3_store().unwrap();
    overwrite_block(store).unwrap();
}

#[test]
#[serial(sftp)]
#[cfg(feature = "store-sftp")]
fn sftp_overwrite_block() {
    let store = sftp_store().unwrap();
    overwrite_block(store).unwrap();
}

#[test]
#[serial(rclone)]
#[cfg(feature = "store-rclone")]
fn rclone_overwrite_block() {
    let store = rclone_store().unwrap();
    overwrite_block(store).unwrap();
}

fn remove_block(mut store: impl DataStore) -> anyhow::Result<()> {
    let id = Uuid::new_v4();
    store.write_block(id, random_buffer().as_slice())?;
    store.remove_block(id)?;
    assert_eq!(store.read_block(id)?, None);

    // Removing a nonexistent block should return `Ok`.
    store.remove_block(Uuid::new_v4())?;

    Ok(())
}

#[test]
fn memory_remove_block() -> anyhow::Result<()> {
    remove_block(MemoryStore::new())
}

#[test]
#[cfg(feature = "store-directory")]
fn directory_remove_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let store = directory_store(temp_dir.as_ref())?;
    remove_block(store)
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_remove_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let store = sqlite_store(temp_dir.as_ref())?;
    remove_block(store)
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_remove_block() {
    let store = redis_store().unwrap();
    remove_block(store).unwrap();
}

#[test]
#[serial(s3)]
#[cfg(feature = "store-s3")]
fn s3_remove_block() {
    let store = s3_store().unwrap();
    remove_block(store).unwrap();
}

#[test]
#[serial(sftp)]
#[cfg(feature = "store-sftp")]
fn sftp_remove_block() {
    let store = sftp_store().unwrap();
    remove_block(store).unwrap();
}

#[test]
#[serial(rclone)]
#[cfg(feature = "store-rclone")]
fn rclone_remove_block() {
    let store = rclone_store().unwrap();
    remove_block(store).unwrap();
}

fn list_blocks(mut store: impl DataStore) -> anyhow::Result<()> {
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let id3 = Uuid::new_v4();

    assert_eq!(store.list_blocks()?, Vec::new());

    store.write_block(id1, random_buffer().as_slice())?;
    store.write_block(id2, random_buffer().as_slice())?;
    store.write_block(id3, random_buffer().as_slice())?;

    let actual_ids = store.list_blocks()?;
    let expected_ids = vec![id1, id2, id3];

    assert_contains_all(actual_ids, expected_ids);

    Ok(())
}

#[test]
fn memory_list_blocks() -> anyhow::Result<()> {
    list_blocks(MemoryStore::new())
}

#[test]
#[cfg(feature = "store-directory")]
fn directory_list_blocks() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let store = directory_store(temp_dir.as_ref())?;
    list_blocks(store)
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_list_blocks() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let store = sqlite_store(temp_dir.as_ref())?;
    list_blocks(store)
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_list_blocks() {
    let store = redis_store().unwrap();
    list_blocks(store).unwrap();
}

#[test]
#[serial(s3)]
#[cfg(feature = "store-s3")]
fn s3_list_blocks() {
    let store = s3_store().unwrap();
    list_blocks(store).unwrap();
}

#[test]
#[serial(sftp)]
#[cfg(feature = "store-sftp")]
fn sftp_list_blocks() {
    let store = sftp_store().unwrap();
    list_blocks(store).unwrap();
}

#[test]
#[serial(rclone)]
#[cfg(feature = "store-rclone")]
fn rclone_list_block() {
    let store = rclone_store().unwrap();
    list_blocks(store).unwrap();
}
