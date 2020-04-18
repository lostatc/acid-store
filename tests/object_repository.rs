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

use std::io::Write;

use tempfile::tempdir;

use acid_store::repo::{LockStrategy, ObjectRepository, OpenRepo, RepositoryConfig};
use acid_store::store::{DataStore, DirectoryStore, MemoryStore, OpenOption, OpenStore};
use common::{create_repo, random_buffer, PASSWORD, REPO_CONFIG};

mod common;

#[test]
fn creating_existing_repo_errs() -> anyhow::Result<()> {
    let initial_repo = create_repo()?;
    let new_repo = ObjectRepository::<String, _>::new_repo(
        initial_repo.into_store(),
        REPO_CONFIG.to_owned(),
        Some(PASSWORD),
    );

    assert!(matches!(
        new_repo.unwrap_err(),
        acid_store::Error::AlreadyExists
    ));
    Ok(())
}

#[test]
fn opening_nonexistent_repo_errs() {
    let repository = ObjectRepository::<String, _>::open_repo(
        MemoryStore::new(),
        LockStrategy::Abort,
        Some(PASSWORD),
    );

    assert!(matches!(
        repository.unwrap_err(),
        acid_store::Error::NotFound
    ));
}

#[test]
fn opening_with_invalid_password_errs() -> anyhow::Result<()> {
    let repository = create_repo()?;
    let repository = ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        LockStrategy::Abort,
        Some(b"not the password"),
    );

    assert!(matches!(
        repository.unwrap_err(),
        acid_store::Error::Password
    ));
    Ok(())
}

#[test]
fn opening_without_password_errs() -> anyhow::Result<()> {
    let repository =
        ObjectRepository::<String, _>::new_repo(MemoryStore::new(), REPO_CONFIG.to_owned(), None);
    assert!(matches!(
        repository.unwrap_err(),
        acid_store::Error::Password
    ));
    Ok(())
}

#[test]
fn opening_with_unnecessary_password_errs() -> anyhow::Result<()> {
    let repository = ObjectRepository::<String, _>::new_repo(
        MemoryStore::new(),
        Default::default(),
        Some(b"unnecessary password"),
    );
    assert!(matches!(
        repository.unwrap_err(),
        acid_store::Error::Password
    ));
    Ok(())
}

#[test]
fn opening_locked_repo_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;

    let store = DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::CREATE_NEW)?;
    let store_copy = DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::empty())?;

    let mut repository =
        ObjectRepository::<String, _>::new_repo(store, REPO_CONFIG.to_owned(), Some(PASSWORD))?;
    repository.commit()?;

    let open_attempt =
        ObjectRepository::<String, _>::open_repo(store_copy, LockStrategy::Abort, Some(PASSWORD));

    assert!(matches!(
        open_attempt.unwrap_err(),
        acid_store::Error::Locked
    ));
    Ok(())
}

#[test]
fn creating_locked_repo_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;

    let store = DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::CREATE_NEW)?;
    let store_copy = DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::empty())?;

    let mut repository =
        ObjectRepository::<String, _>::new_repo(store, REPO_CONFIG.to_owned(), Some(PASSWORD))?;
    repository.commit()?;

    let open_attempt =
        ObjectRepository::<String, _>::new_repo(store_copy, REPO_CONFIG.to_owned(), Some(PASSWORD));

    assert!(matches!(
        open_attempt.unwrap_err(),
        acid_store::Error::AlreadyExists
    ));
    Ok(())
}

#[test]
fn opening_with_wrong_key_type_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Test".into());
    repository.commit()?;

    let repository = ObjectRepository::<isize, _>::open_repo(
        repository.into_store(),
        LockStrategy::Abort,
        Some(PASSWORD),
    );

    assert!(matches!(
        repository.unwrap_err(),
        acid_store::Error::KeyType
    ));
    Ok(())
}

#[test]
fn inserted_key_replaces_existing_key() -> anyhow::Result<()> {
    // Insert an object and write data to it.
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;

    assert_ne!(object.size(), 0);

    // Replace the object with an empty one.
    drop(object);
    let object = repository.insert("Test".into());

    assert_eq!(object.size(), 0);

    Ok(())
}

#[test]
fn remove_object() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Test".into());

    assert!(repository.remove("Test"));
    assert!(!repository.remove("Test"));

    Ok(())
}

#[test]
fn copied_object_has_same_contents() -> anyhow::Result<()> {
    // Write data to an object.
    let mut repository = create_repo()?;
    let mut object = repository.insert("Source".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    let source_id = object.content_id();
    drop(object);

    // Copy the object.
    repository.copy("Source", "Dest".into())?;
    let object = repository.get("Dest").unwrap();
    let dest_id = object.content_id();

    assert_eq!(source_id, dest_id);

    Ok(())
}

#[test]
fn copied_object_must_exist() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    assert!(matches!(
        repository.copy("Nonexistent", "Dest".into()).unwrap_err(),
        acid_store::Error::NotFound
    ));
    Ok(())
}

#[test]
fn copying_does_not_overwrite() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Source".into());
    repository.insert("Dest".into());

    assert!(matches!(
        repository.copy("Source", "Dest".into()).unwrap_err(),
        acid_store::Error::AlreadyExists
    ));

    Ok(())
}

#[test]
fn committed_changes_are_persisted() -> anyhow::Result<()> {
    // Write data to the repository.
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    let expected_id = object.content_id();

    drop(object);
    repository.commit()?;

    // Re-open the repository.
    let repository = ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        LockStrategy::Abort,
        Some(PASSWORD),
    )?;
    let object = repository.get("Test".into()).unwrap();
    let actual_id = object.content_id();

    assert_eq!(actual_id, expected_id);

    Ok(())
}

#[test]
fn uncommitted_changes_are_not_persisted() -> anyhow::Result<()> {
    // Write data to the repository.
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;

    drop(object);

    // Re-open the repository.
    let repository = ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        LockStrategy::Abort,
        Some(PASSWORD),
    )?;

    assert!(repository.get("Test".into()).is_none());

    Ok(())
}

#[test]
fn unused_data_is_reclaimed_on_commit() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);
    repository.commit()?;

    let mut store = repository.into_store();
    let original_blocks = store.list_blocks()?.len();

    let mut repository =
        ObjectRepository::<String, _>::open_repo(store, LockStrategy::Abort, Some(PASSWORD))?;
    repository.remove("Test");
    repository.commit()?;

    let mut store = repository.into_store();
    let new_blocks = store.list_blocks()?.len();

    assert!(new_blocks < original_blocks);
    Ok(())
}

#[test]
fn verify_valid_repository_is_valid() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let corrupt_keys = repository.verify()?;

    assert!(corrupt_keys.is_empty());
    Ok(())
}

#[test]
fn change_password() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.change_password(b"new password");
    repository.commit()?;

    ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        LockStrategy::Abort,
        Some(b"new password"),
    )?;

    Ok(())
}

#[test]
fn calculate_apparent_and_actual_size() -> anyhow::Result<()> {
    // Create a repository with compression and encryption disabled.
    let mut repository =
        ObjectRepository::new_repo(MemoryStore::new(), RepositoryConfig::default(), None)?;
    let data = random_buffer();

    let mut object = repository.insert("Test1".to_string());
    object.write_all(data.as_slice())?;
    object.flush()?;
    drop(object);

    let mut object = repository.insert("Test2".to_string());
    object.write_all(data.as_slice())?;
    object.flush()?;
    drop(object);

    let stats = repository.stats();
    assert_eq!(stats.apparent_size(), data.len() as u64 * 2);
    assert_eq!(stats.actual_size(), data.len() as u64);

    Ok(())
}

#[test]
fn peek_info() -> anyhow::Result<()> {
    let repository = create_repo()?;
    let expected_info = repository.info();
    let mut store = repository.into_store();
    let actual_info = ObjectRepository::<String, _>::peek_info(&mut store)?;

    assert_eq!(actual_info, expected_info);
    Ok(())
}
