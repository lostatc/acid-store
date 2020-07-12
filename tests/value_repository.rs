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
use acid_store::repo::{ConvertRepo, OpenOptions};
use acid_store::store::MemoryStore;
use common::assert_contains_all;

mod common;

/// A serializable value to test with.
const SERIALIZABLE_VALUE: (bool, i32) = (true, 42);

fn create_repo() -> acid_store::Result<ValueRepo<String, MemoryStore>> {
    OpenOptions::new(MemoryStore::new()).create_new()
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.commit()?;
    let store = repository.into_repo()?.into_store();
    OpenOptions::new(store).open::<ValueRepo<String, _>>()?;
    Ok(())
}

#[test]
fn insert_value() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Key".into(), &SERIALIZABLE_VALUE)?;
    let actual: (bool, i32) = repository.get("Key")?;
    assert_eq!(actual, SERIALIZABLE_VALUE);
    Ok(())
}

#[test]
fn remove_value() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

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
    let mut repository = create_repo()?;
    repository.insert("Key".into(), &SERIALIZABLE_VALUE)?;
    let actual = repository.get::<_, String>("Key");
    assert!(matches!(actual, Err(acid_store::Error::Deserialize)));
    Ok(())
}

#[test]
fn list_keys() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Key1".into(), &SERIALIZABLE_VALUE)?;
    repository.insert("Key2".into(), &SERIALIZABLE_VALUE)?;
    repository.insert("Key3".into(), &SERIALIZABLE_VALUE)?;

    let expected = vec!["Key1".to_string(), "Key2".to_string(), "Key3".to_string()];
    let actual = repository.keys().cloned().collect::<Vec<_>>();

    assert_contains_all(actual, expected);
    Ok(())
}

#[test]
fn verify_valid_repository_is_valid() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Test".into(), &SERIALIZABLE_VALUE)?;

    assert!(repository.verify()?.is_empty());
    Ok(())
}
