/*
 * Copyright 2019-2020 Garrett Powell
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

use std::io::{Read, Write};

use matches::assert_matches;

use acid_store::repo::{LockStrategy, VersionRepository};
use acid_store::store::MemoryStore;
use common::{assert_contains_all, random_buffer, PASSWORD, REPO_CONFIG};

mod common;

fn create_repo() -> acid_store::Result<VersionRepository<String, MemoryStore>> {
    VersionRepository::create_repo(MemoryStore::new(), REPO_CONFIG.to_owned(), Some(PASSWORD))
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.commit()?;
    let store = repository.into_store();
    VersionRepository::<String, _>::open_repo(store, Some(PASSWORD), LockStrategy::Abort)?;
    Ok(())
}

#[test]
fn get_version() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    // Add a new object and write data to it.
    let expected_data = random_buffer();
    let mut object = repository.insert("Key".into())?;
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);

    // Create a new version of the object.
    let version = repository.create_version("Key".into())?;

    // Read the new version.
    let mut object = repository
        .get_version("Key", version.id())
        .ok_or(acid_store::Error::NotFound)?;
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test]
fn list_versions() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.insert("Key".into())?;
    let version1 = repository.create_version("Key".into())?;
    let version2 = repository.create_version("Key".into())?;
    let version3 = repository.create_version("Key".into())?;

    let expected = vec![version1, version2, version3];
    let actual = repository.list_versions("Key")?;

    assert_contains_all(actual, expected);

    Ok(())
}

#[test]
fn remove_version() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.insert("Key".into())?;
    let version = repository.create_version("Key".into())?;
    repository.remove_version("Key", version.id())?;

    assert!(repository.get_version("Key", version.id()).is_none());
    Ok(())
}

#[test]
fn remove_and_list_versions() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.insert("Key".into())?;
    let version1 = repository.create_version("Key".into())?;
    let version2 = repository.create_version("Key".into())?;
    let version3 = repository.create_version("Key".into())?;
    repository.remove_version("Key", version2.id())?;

    let expected = vec![version1, version3];
    let actual = repository.list_versions("Key")?;

    assert_contains_all(actual, expected);

    Ok(())
}

#[test]
fn versioning_nonexistent_key_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    assert_matches!(
        repository.create_version("Key".into()),
        Err(acid_store::Error::NotFound)
    );
    assert_matches!(
        repository.remove_version("Key", 1),
        Err(acid_store::Error::NotFound)
    );
    assert_matches!(
        repository.list_versions("Key"),
        Err(acid_store::Error::NotFound)
    );
    Ok(())
}

#[test]
fn removing_key_removes_versions() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.insert("Key".into())?;
    let version = repository.create_version("Key".into())?;
    repository.remove("Key")?;

    assert!(repository.get_version("Key", version.id()).is_none());
    assert_matches!(
        repository.list_versions("Key"),
        Err(acid_store::Error::NotFound)
    );
    Ok(())
}

#[test]
fn restore_version() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    let expected_data = random_buffer();

    // Create an object and write data to it.
    let mut object = repository.insert("Key".into())?;
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);

    // Create a new version.
    let version = repository.create_version("Key".into())?;

    // Modify the contents of the object.
    let mut object = repository.get_mut("Key").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    // Restore the contents from the version.
    repository.restore_version("Key", version.id())?;

    // Check the contents.
    let mut actual_data = Vec::new();
    let mut object = repository.get("Key").unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);
    Ok(())
}

#[test]
fn modifying_object_doesnt_modify_versions() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.insert("Key".into())?;
    let version = repository.create_version("Key".into())?;

    let mut object = repository.get_mut("Key").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let object = repository.get_version("Key", version.id()).unwrap();
    assert_eq!(object.size(), 0);

    Ok(())
}
