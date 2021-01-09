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

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{OpenMode, OpenOptions, SwitchInstance, DEFAULT_INSTANCE};
use acid_store::store::MemoryConfig;
use acid_store::uuid::Uuid;
use common::random_buffer;

mod common;

fn create_repo(config: &MemoryConfig) -> acid_store::Result<KeyRepo<String>> {
    OpenOptions::new().mode(OpenMode::CreateNew).open(config)
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;
    repo.commit()?;
    drop(repo);
    OpenOptions::new().open::<KeyRepo<String>, _>(&config)?;
    Ok(())
}

#[test]
fn switching_instance_does_not_roll_back() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let mut object = repo.insert("test".to_string());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let repo: KeyRepo<String> = repo.set_instance(Uuid::new_v4())?;
    let repo: KeyRepo<String> = repo.set_instance(DEFAULT_INSTANCE)?;

    assert!(repo.contains("test"));
    assert!(repo.object("test").is_some());

    Ok(())
}

#[test]
fn switching_instance_does_not_commit() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let mut object = repo.insert("test".to_string());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let repo: KeyRepo<String> = repo.set_instance(Uuid::new_v4())?;
    let mut repo: KeyRepo<String> = repo.set_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test]
fn opening_with_wrong_key_type_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;
    repo.insert("Test".into());
    repo.commit()?;
    drop(repo);

    let repo: Result<KeyRepo<isize>, _> = OpenOptions::new().open(&config);

    assert!(matches!(repo, Err(acid_store::Error::Deserialize)));
    Ok(())
}

#[test]
fn inserted_key_replaces_existing_key() -> anyhow::Result<()> {
    // Insert an object and write data to it.
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;
    let mut object = repo.insert("Test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;

    assert_ne!(object.size(), 0);

    // Replace the object with an empty one.
    drop(object);
    let object = repo.insert("Test".into());

    assert_eq!(object.size(), 0);

    Ok(())
}

#[test]
fn remove_object() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;
    repo.insert("Test".into());

    assert!(repo.remove("Test"));
    assert!(!repo.remove("Test"));

    Ok(())
}

#[test]
fn copied_object_has_same_contents() -> anyhow::Result<()> {
    // Write data to an object.
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;
    let mut object = repo.insert("Source".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    let source_id = object.content_id();
    drop(object);

    // Copy the object.
    repo.copy("Source", "Dest".into())?;
    let object = repo.object("Dest").unwrap();
    let dest_id = object.content_id();

    assert_eq!(source_id, dest_id);

    Ok(())
}

#[test]
fn copied_object_must_exist() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;
    assert!(matches!(
        repo.copy("Nonexistent", "Dest".into()),
        Err(acid_store::Error::NotFound)
    ));
    Ok(())
}

#[test]
fn copying_does_not_overwrite() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;
    repo.insert("Source".into());
    repo.insert("Dest".into());

    assert!(matches!(
        repo.copy("Source", "Dest".into()),
        Err(acid_store::Error::AlreadyExists)
    ));

    Ok(())
}

#[test]
fn objects_removed_on_rollback() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let mut object = repo.insert("test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.rollback()?;

    assert!(!repo.contains("test"));
    assert!(repo.keys().next().is_none());
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test]
fn clear_instance_removes_keys() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let mut object = repo.insert("test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.clear_instance();

    assert!(!repo.contains("test"));
    assert!(repo.keys().next().is_none());
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test]
fn rollback_after_clear_instance() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    let mut object = repo.insert("test".into());
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert!(repo.contains("test"));
    assert!(repo.keys().next().is_some());
    assert!(repo.object("test").is_some());

    Ok(())
}
