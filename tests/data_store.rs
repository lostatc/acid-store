/*
 * Copyright 2019 Garrett Powell
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

use std::collections::HashSet;

#[cfg(feature = "store-redis")]
use redis::IntoConnectionInfo;
use serial_test::serial;
use tempfile::tempdir;
use uuid::Uuid;

use common::random_buffer;
#[cfg(feature = "store-directory")]
use data_store::store::DirectoryStore;
#[cfg(feature = "store-redis")]
use data_store::store::RedisStore;
#[cfg(feature = "store-sqlite")]
use data_store::store::SqliteStore;
use data_store::store::{DataStore, MemoryStore, Open, OpenOption};

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
    read_block(DirectoryStore::open(
        temp_dir.as_ref().join("store"),
        OpenOption::CREATE_NEW,
    )?)
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_read_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    read_block(SqliteStore::open(
        temp_dir.as_ref().join("store.db"),
        OpenOption::CREATE_NEW,
    )?)
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_read_block() {
    let redis_url = dotenv::var("REDIS_TEST_URL").unwrap();
    read_block(
        RedisStore::open(
            redis_url.into_connection_info().unwrap(),
            OpenOption::CREATE | OpenOption::TRUNCATE,
        )
        .unwrap(),
    )
    .unwrap();
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
    overwrite_block(DirectoryStore::open(
        temp_dir.as_ref().join("store"),
        OpenOption::CREATE_NEW,
    )?)
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_overwrite_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    overwrite_block(SqliteStore::open(
        temp_dir.as_ref().join("store.db"),
        OpenOption::CREATE_NEW,
    )?)
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_overwrite_block() {
    let redis_url = dotenv::var("REDIS_TEST_URL").unwrap();
    overwrite_block(
        RedisStore::open(
            redis_url.into_connection_info().unwrap(),
            OpenOption::CREATE | OpenOption::TRUNCATE,
        )
        .unwrap(),
    )
    .unwrap();
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
    remove_block(DirectoryStore::open(
        temp_dir.as_ref().join("store"),
        OpenOption::CREATE_NEW,
    )?)
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_remove_block() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    remove_block(SqliteStore::open(
        temp_dir.as_ref().join("store.db"),
        OpenOption::CREATE_NEW,
    )?)
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_remove_block() {
    let redis_url = dotenv::var("REDIS_TEST_URL").unwrap();
    remove_block(
        RedisStore::open(
            redis_url.into_connection_info().unwrap(),
            OpenOption::CREATE | OpenOption::TRUNCATE,
        )
        .unwrap(),
    )
    .unwrap();
}

fn list_blocks(mut store: impl DataStore) -> anyhow::Result<()> {
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let id3 = Uuid::new_v4();

    assert_eq!(store.list_blocks()?, Vec::new());

    store.write_block(id1, random_buffer().as_slice())?;
    store.write_block(id2, random_buffer().as_slice())?;
    store.write_block(id3, random_buffer().as_slice())?;

    let actual_ids = store.list_blocks()?.into_iter().collect::<HashSet<_>>();
    let expected_ids = vec![id1, id2, id3].into_iter().collect::<HashSet<_>>();

    assert_eq!(actual_ids, expected_ids);

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
    list_blocks(DirectoryStore::open(
        temp_dir.as_ref().join("store"),
        OpenOption::CREATE_NEW,
    )?)
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_list_blocks() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    list_blocks(SqliteStore::open(
        temp_dir.as_ref().join("store.db"),
        OpenOption::CREATE_NEW,
    )?)
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_list_blocks() {
    let redis_url = dotenv::var("REDIS_TEST_URL").unwrap();
    list_blocks(
        RedisStore::open(
            redis_url.into_connection_info().unwrap(),
            OpenOption::CREATE | OpenOption::TRUNCATE,
        )
        .unwrap(),
    )
    .unwrap();
}
