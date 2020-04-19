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

use tempfile::tempdir;
use uuid::Uuid;

use acid_store::store::{
    DataStore, DirectoryStore, MemoryStore, MultiStore, OpenOption, OpenStore,
};

#[test]
fn multi_store_is_persistent() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let backing_store =
        DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::CREATE_NEW)?;
    let mut multi_store = MultiStore::new(backing_store)?;

    let id = Uuid::new_v4();

    // Open a proxy store and write to it.
    let mut proxy_store = multi_store.insert(String::from("Test"))?;
    proxy_store.write_block(id, b"data")?;
    drop(proxy_store);

    // Open the same proxy store and read from it.
    let backing_store = multi_store.into_store();
    let mut multi_store = MultiStore::<String, _>::new(backing_store)?;
    let mut proxy_store = multi_store.get("Test")?;
    assert_eq!(proxy_store.read_block(id)?.unwrap().as_slice(), b"data");

    Ok(())
}

#[test]
fn removing_data_store_removes_blocks() -> anyhow::Result<()> {
    let mut multi_store = MultiStore::new(MemoryStore::new())?;
    let mut proxy_store = multi_store.insert(String::from("Test"))?;

    proxy_store.write_block(Uuid::new_v4(), b"data")?;
    proxy_store.write_block(Uuid::new_v4(), b"data")?;
    proxy_store.write_block(Uuid::new_v4(), b"data")?;
    drop(proxy_store);

    let mut backing_store = multi_store.into_store();
    let num_blocks_before = backing_store.list_blocks()?.len();

    let mut multi_store = MultiStore::<String, _>::new(backing_store)?;
    multi_store.remove("Test")?;
    let mut backing_store = multi_store.into_store();
    let num_blocks_after = backing_store.list_blocks()?.len();

    assert_eq!(num_blocks_before, num_blocks_after + 3);
    Ok(())
}
