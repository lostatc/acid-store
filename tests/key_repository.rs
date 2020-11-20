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
use acid_store::repo::{ConvertRepo, OpenOptions};
use acid_store::store::MemoryStore;
use common::random_buffer;

mod common;

fn create_repo() -> acid_store::Result<KeyRepo<String, MemoryStore>> {
    OpenOptions::new(MemoryStore::new()).create_new()
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    repo.commit()?;
    let store = repo.into_repo()?.into_store();
    OpenOptions::new(store).open::<KeyRepo<String, _>>()?;
    Ok(())
}

#[test]
fn opening_with_wrong_key_type_errs() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    repo.insert("Test".into());
    repo.commit()?;

    let store = repo.into_repo()?.into_store();
    let repo: Result<KeyRepo<isize, _>, _> = OpenOptions::new(store).open();

    assert!(matches!(repo, Err(acid_store::Error::Deserialize)));
    Ok(())
}

#[test]
fn inserted_key_replaces_existing_key() -> anyhow::Result<()> {
    // Insert an object and write data to it.
    let mut repo = create_repo()?;
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
    let mut repo = create_repo()?;
    repo.insert("Test".into());

    assert!(repo.remove("Test"));
    assert!(!repo.remove("Test"));

    Ok(())
}

#[test]
fn copied_object_has_same_contents() -> anyhow::Result<()> {
    // Write data to an object.
    let mut repo = create_repo()?;
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
    let mut repo = create_repo()?;
    assert!(matches!(
        repo.copy("Nonexistent", "Dest".into()),
        Err(acid_store::Error::NotFound)
    ));
    Ok(())
}

#[test]
fn copying_does_not_overwrite() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
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
    let mut repo = create_repo()?;

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
