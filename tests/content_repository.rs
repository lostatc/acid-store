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

use acid_store::repo::content::{ContentRepo, HashAlgorithm};
use acid_store::repo::{OpenMode, OpenOptions, SwitchInstance, DEFAULT_INSTANCE};
use acid_store::store::MemoryConfig;
use acid_store::uuid::Uuid;
use common::random_buffer;

mod common;

fn create_repo(config: &MemoryConfig) -> acid_store::Result<ContentRepo> {
    OpenOptions::new().mode(OpenMode::CreateNew).open(config)
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.commit()?;
    drop(repository);
    OpenOptions::new().open::<ContentRepo, _>(&config)?;
    Ok(())
}

#[test]
fn switching_instance_does_not_roll_back() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let hash = repo.put(random_buffer().as_slice())?;

    let repo: ContentRepo = repo.switch_instance(Uuid::new_v4())?;
    let repo: ContentRepo = repo.switch_instance(DEFAULT_INSTANCE)?;

    assert!(repo.contains(&hash));
    assert!(repo.object(&hash).is_some());

    Ok(())
}

#[test]
fn switching_instance_does_not_commit() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let hash = repo.put(random_buffer().as_slice())?;

    let repo: ContentRepo = repo.switch_instance(Uuid::new_v4())?;
    let mut repo: ContentRepo = repo.switch_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert!(!repo.contains(&hash));
    assert!(repo.object(&hash).is_none());

    Ok(())
}

#[test]
fn put_object() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    let data = random_buffer();
    let hash = repository.put(data.as_slice())?;

    assert!(repository.contains(hash.as_slice()));
    Ok(())
}

#[test]
fn remove_object() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    let data = random_buffer();
    let hash = repository.put(data.as_slice())?;
    repository.remove(&hash);

    assert!(!repository.contains(hash.as_slice()));
    Ok(())
}

#[test]
fn get_object() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    let expected_data = random_buffer();
    let hash = repository.put(expected_data.as_slice())?;

    let mut object = repository.object(&hash).unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_eq!(actual_data, expected_data);
    Ok(())
}

#[test]
fn list_objects() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
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
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    let expected_data = b"Data";
    repository.put(&expected_data[..])?;

    repository.change_algorithm(HashAlgorithm::Blake2b(4))?;
    let expected_hash: &[u8] = &[228, 220, 4, 124];

    let mut object = repository.object(&expected_hash).unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_eq!(actual_data.as_slice(), expected_data);
    Ok(())
}

#[test]
fn objects_removed_on_rollback() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    let hash = repository.put(random_buffer().as_slice())?;

    repository.rollback()?;

    assert!(!repository.contains(&hash));
    assert!(repository.object(&hash).is_none());
    assert!(repository.list().next().is_none());

    Ok(())
}

#[test]
fn clear_instance_removes_keys() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let hash = repo.put(random_buffer().as_slice())?;

    repo.clear_instance();

    assert!(!repo.contains(&hash));
    assert!(repo.list().next().is_none());
    assert!(repo.object(&hash).is_none());

    Ok(())
}

#[test]
fn rollback_after_clear_instance() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let hash = repo.put(random_buffer().as_slice())?;

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert!(repo.contains(&hash));
    assert!(repo.list().next().is_some());
    assert!(repo.object(&hash).is_some());

    Ok(())
}

#[test]
fn verify_valid_repository_is_valid() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.put(random_buffer().as_slice())?;

    assert!(repository.verify()?.is_empty());
    Ok(())
}
