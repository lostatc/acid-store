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

use std::io::{Read, Write};

use acid_store::repo::version::VersionRepo;
use acid_store::repo::{OpenMode, OpenOptions};
use acid_store::store::MemoryConfig;
use common::{assert_contains_all, random_buffer};

mod common;

fn create_repo(config: &MemoryConfig) -> acid_store::Result<VersionRepo<String>> {
    OpenOptions::new().mode(OpenMode::CreateNew).open(config)
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.commit()?;
    drop(repository);
    OpenOptions::new().open::<VersionRepo<String>, _>(&config)?;
    Ok(())
}

#[test]
fn read_version() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    // Add a new object and write data to it.
    let expected_data = random_buffer();
    let mut object = repository.insert(String::from("Key")).unwrap();
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);

    // Create a new version of the object.
    let version = repository.create_version("Key").unwrap();

    // Read the new version.
    let mut object = repository
        .version_object("Key", version.id())
        .ok_or(acid_store::Error::NotFound)?;
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test]
fn list_versions() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    repository.insert("Key".into()).unwrap();
    let version1 = repository.create_version("Key").unwrap();
    let version2 = repository.create_version("Key").unwrap();
    let version3 = repository.create_version("Key").unwrap();

    let expected = vec![version1.id(), version2.id(), version3.id()];
    let versions = repository.versions("Key").unwrap();
    let actual = versions.map(|version| version.id());

    assert_contains_all(actual, expected);

    Ok(())
}

#[test]
fn remove_version() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    repository.insert(String::from("Key")).unwrap();
    let version = repository.create_version("Key").unwrap();
    repository.remove_version("Key", version.id());

    assert!(repository.version_object("Key", version.id()).is_none());
    Ok(())
}

#[test]
fn remove_and_list_versions() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    repository.insert("Key".into()).unwrap();
    let version1 = repository.create_version("Key").unwrap();
    let version2 = repository.create_version("Key").unwrap();
    let version3 = repository.create_version("Key").unwrap();
    repository.remove_version("Key", version2.id());

    let expected = vec![version1.id(), version3.id()];
    let versions = repository.versions("Key").unwrap();
    let actual = versions.map(|version| version.id());

    assert_contains_all(actual, expected);

    Ok(())
}

#[test]
fn remove_and_get_version() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    repository.insert("Key".into()).unwrap();
    let version = repository.create_version("Key").unwrap();

    assert_eq!(
        repository.get_version("Key", version.id()).unwrap(),
        version
    );
    assert!(repository.remove_version("Key", version.id()));
    assert!(repository.get_version("Key", version.id()).is_none());

    Ok(())
}

#[test]
fn versioning_nonexistent_key_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    assert!(repository.create_version("Key").is_none());
    assert!(!repository.remove_version("Key", 1));
    assert!(repository.versions("Key").is_none());
    Ok(())
}

#[test]
fn removing_key_removes_versions() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    repository.insert("Key".into()).unwrap();
    let version = repository.create_version("Key").unwrap();
    repository.remove("Key");

    assert!(repository.version_object("Key", version.id()).is_none());
    assert!(repository.versions("Key").is_none());
    assert!(repository.get_version("Key", version.id()).is_none());
    Ok(())
}

#[test]
fn restore_version() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    let expected_data = random_buffer();

    // Create an object and write data to it.
    let mut object = repository.insert("Key".into()).unwrap();
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);

    // Create a new version.
    let version = repository.create_version("Key").unwrap();

    // Modify the contents of the object.
    let mut object = repository.object_mut("Key").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    // Restore the contents from the version.
    assert!(repository.restore_version("Key", version.id()));

    // Check the contents.
    let mut actual_data = Vec::new();
    let mut object = repository.object("Key").unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);
    Ok(())
}

#[test]
fn modifying_object_doesnt_modify_versions() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    repository.insert(String::from("Key")).unwrap();
    let version = repository.create_version("Key").unwrap();

    let mut object = repository.object_mut("Key").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let object = repository.version_object("Key", version.id()).unwrap();
    assert_eq!(object.size(), 0);

    Ok(())
}

#[test]
fn objects_removed_on_rollback() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;

    let mut object = repository.insert("test".into()).unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repository.create_version("test").unwrap();

    repository.rollback()?;

    assert!(!repository.contains("test"));
    assert!(repository.keys().next().is_none());
    assert!(repository.object("test").is_none());

    Ok(())
}
