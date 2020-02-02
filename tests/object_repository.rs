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

use std::io::Write;

use matches::assert_matches;

use acid_store::repo::{LockStrategy, ObjectRepository};
use acid_store::store::MemoryStore;
use common::{create_repo, random_buffer, ARCHIVE_CONFIG, PASSWORD};

#[macro_use]
mod common;

#[test]
fn creating_existing_repo_errs() -> anyhow::Result<()> {
    let initial_repo = create_repo()?;
    let new_repo = ObjectRepository::<String, _>::create_repo(
        initial_repo.into_store(),
        ARCHIVE_CONFIG,
        Some(PASSWORD),
    );

    assert_matches!(new_repo.unwrap_err(), acid_store::Error::AlreadyExists);
    Ok(())
}

#[test]
fn opening_nonexistent_repo_errs() {
    let repository = ObjectRepository::<String, _>::open_repo(
        MemoryStore::new(),
        Some(PASSWORD),
        LockStrategy::Abort,
    );

    assert_matches!(repository.unwrap_err(), acid_store::Error::NotFound);
}

#[test]
fn opening_with_invalid_password_errs() -> anyhow::Result<()> {
    let repository = create_repo()?;
    let repository = ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        Some(b"not the password"),
        LockStrategy::Abort,
    );

    assert_matches!(repository.unwrap_err(), acid_store::Error::Password);
    Ok(())
}

#[test]
fn opening_with_wrong_key_type_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Test".into());
    repository.commit()?;

    let repository = ObjectRepository::<isize, _>::open_repo(
        repository.into_store(),
        Some(PASSWORD),
        LockStrategy::Abort,
    );

    assert_matches!(repository.unwrap_err(), acid_store::Error::KeyType);
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

    // Copy the object.
    repository.copy("Source", "Dest".into())?;
    let object = repository.get("Dest".into()).unwrap();
    let dest_id = object.content_id();

    assert_eq!(source_id, dest_id);

    Ok(())
}

#[test]
fn copied_object_must_exist() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    assert_matches!(
        repository.copy("Nonexistent", "Dest".into()).unwrap_err(),
        acid_store::Error::NotFound
    );
    Ok(())
}

#[test]
fn copying_does_not_overwrite() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Source".into());
    repository.insert("Dest".into());

    assert_matches!(
        repository.copy("Source", "Dest".into()).unwrap_err(),
        acid_store::Error::AlreadyExists
    );

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
    let mut repository = ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        Some(PASSWORD),
        LockStrategy::Abort,
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
    let mut repository = ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        Some(PASSWORD),
        LockStrategy::Abort,
    )?;

    assert!(repository.get("Test".into()).is_none());

    Ok(())
}

#[test]
fn change_password() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.change_password(b"new password");
    repository.commit()?;

    ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        Some(b"new password"),
        LockStrategy::Abort,
    )?;

    Ok(())
}
