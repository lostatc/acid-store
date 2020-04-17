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

use std::collections::HashSet;
use std::io::Read;

use acid_store::repo::{ContentRepository, HashAlgorithm, LockStrategy, OpenRepo};
use acid_store::store::MemoryStore;
use common::{random_buffer, PASSWORD, REPO_CONFIG};

mod common;

fn create_repo() -> acid_store::Result<ContentRepository<MemoryStore>> {
    ContentRepository::create_new_repo(MemoryStore::new(), REPO_CONFIG.to_owned(), Some(PASSWORD))
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.commit()?;
    let store = repository.into_store();
    ContentRepository::open_repo(store, LockStrategy::Abort, Some(PASSWORD))?;
    Ok(())
}

#[test]
fn put_object() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let data = random_buffer();
    let hash = repository.put(data.as_slice())?;

    assert!(repository.contains(hash.as_slice()));
    Ok(())
}

#[test]
fn remove_object() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let data = random_buffer();
    let hash = repository.put(data.as_slice())?;
    repository.remove(&hash);

    assert!(!repository.contains(hash.as_slice()));
    Ok(())
}

#[test]
fn get_object() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let expected_data = random_buffer();
    let hash = repository.put(expected_data.as_slice())?;

    let mut object = repository.get(&hash).unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_eq!(actual_data, expected_data);
    Ok(())
}

#[test]
fn list_objects() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let hash1 = repository.put(random_buffer().as_slice())?;
    let hash2 = repository.put(random_buffer().as_slice())?;
    let expected_hashes = vec![hash1, hash2].into_iter().collect::<HashSet<_>>();
    let actual_hashes = repository
        .list()
        .map(|hash| hash.to_vec())
        .collect::<HashSet<_>>();

    assert_eq!(actual_hashes, expected_hashes);
    Ok(())
}

#[test]
fn change_algorithm() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let expected_data = b"Data";
    repository.put(&expected_data[..])?;

    repository.change_algorithm(HashAlgorithm::Blake2b(4))?;
    let expected_hash: &[u8] = &[228, 220, 4, 124];

    let mut object = repository.get(&expected_hash).unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_eq!(actual_data.as_slice(), expected_data);
    Ok(())
}

#[test]
fn verify_valid_repository_is_valid() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.put(random_buffer().as_slice())?;

    assert!(repository.verify()?.is_empty());
    Ok(())
}
