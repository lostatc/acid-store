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

use acid_store::repo::value::ValueRepo;
use acid_store::repo::{OpenMode, OpenOptions, SwitchInstance, DEFAULT_INSTANCE};
use acid_store::store::MemoryConfig;
use acid_store::uuid::Uuid;
use common::assert_contains_all;

mod common;

/// A serializable value to test with.
const SERIALIZABLE_VALUE: (bool, i32) = (true, 42);

fn create_repo(config: &MemoryConfig) -> acid_store::Result<ValueRepo<String>> {
    OpenOptions::new().mode(OpenMode::CreateNew).open(config)
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.commit()?;
    drop(repository);
    OpenOptions::new().open::<ValueRepo<String>, _>(&config)?;
    Ok(())
}

#[test]
fn switching_instance_does_not_roll_back() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    repo.insert("test".to_string(), &SERIALIZABLE_VALUE)?;

    let repo: ValueRepo<String> = repo.switch_instance(Uuid::new_v4())?;
    let repo: ValueRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;

    assert!(repo.contains("test"));
    assert!(repo.get::<_, (bool, i32)>("test").is_ok());

    Ok(())
}

#[test]
fn switching_instance_does_not_commit() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    repo.insert("test".to_string(), &SERIALIZABLE_VALUE)?;

    let repo: ValueRepo<String> = repo.switch_instance(Uuid::new_v4())?;
    let mut repo: ValueRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert!(!repo.contains("test"));
    assert!(matches!(
        repo.get::<_, (bool, i32)>("test"),
        Err(acid_store::Error::NotFound)
    ));

    Ok(())
}

#[test]
fn insert_value() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.insert("Key".into(), &SERIALIZABLE_VALUE)?;
    let actual: (bool, i32) = repository.get("Key")?;
    assert_eq!(actual, SERIALIZABLE_VALUE);
    Ok(())
}

#[test]
fn remove_value() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    assert!(!repository.remove("Key"));
    assert!(!repository.contains("Key"));

    repository.insert("Key".into(), &SERIALIZABLE_VALUE)?;

    assert!(repository.contains("Key"));
    assert!(repository.remove("Key"));
    assert!(!repository.contains("Key"));

    Ok(())
}

#[test]
fn deserializing_value_to_wrong_type_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.insert("Key".into(), &SERIALIZABLE_VALUE)?;
    let actual = repository.get::<_, String>("Key");
    assert!(matches!(actual, Err(acid_store::Error::Deserialize)));
    Ok(())
}

#[test]
fn list_keys() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.insert("Key1".into(), &SERIALIZABLE_VALUE)?;
    repository.insert("Key2".into(), &SERIALIZABLE_VALUE)?;
    repository.insert("Key3".into(), &SERIALIZABLE_VALUE)?;

    let expected = vec!["Key1".to_string(), "Key2".to_string(), "Key3".to_string()];
    let actual = repository.keys().cloned().collect::<Vec<_>>();

    assert_contains_all(actual, expected);
    Ok(())
}

#[test]
fn values_removed_on_rollback() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.insert("test".into(), &SERIALIZABLE_VALUE)?;

    repository.rollback()?;

    assert!(!repository.contains("test"));
    assert!(repository.keys().next().is_none());
    assert!(matches!(
        repository.get::<_, (bool, i32)>("test"),
        Err(acid_store::Error::NotFound)
    ));

    Ok(())
}

#[test]
fn clear_instance_removes_keys() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    repo.insert("test".into(), &SERIALIZABLE_VALUE)?;

    repo.clear_instance();

    assert!(!repo.contains("test"));
    assert!(matches!(
        repo.get::<_, (bool, u32)>("test"),
        Err(acid_store::Error::NotFound)
    ));

    Ok(())
}

#[test]
fn rollback_after_clear_instance() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    repo.insert("test".into(), &SERIALIZABLE_VALUE)?;

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert!(repo.contains("test"));
    assert!(repo.get::<_, (bool, u32)>("test").is_ok());

    Ok(())
}

#[test]
fn verify_valid_repository_is_valid() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.insert("Test".into(), &SERIALIZABLE_VALUE)?;

    assert!(repository.verify()?.is_empty());
    Ok(())
}
