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

#![cfg(feature = "encryption")]

use std::io::{Read, Write};

use acid_store::repo::object::ObjectRepo;
use acid_store::repo::{ConvertRepo, Encryption, OpenOptions};
use acid_store::store::{DataStore, MemoryStore};
use common::random_buffer;

mod common;

fn create_repo() -> acid_store::Result<ObjectRepo<MemoryStore>> {
    OpenOptions::new(MemoryStore::new()).create_new()
}

#[test]
fn change_password() -> anyhow::Result<()> {
    let mut repo: ObjectRepo<_> = OpenOptions::new(MemoryStore::new())
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .create_new()?;
    repo.change_password(b"New password");
    repo.commit()?;

    OpenOptions::new(repo.into_store())
        .password(b"New password")
        .open::<ObjectRepo<_>>()?;

    Ok(())
}

#[test]
fn peek_info() -> anyhow::Result<()> {
    let repository: ObjectRepo<_> = OpenOptions::new(MemoryStore::new()).create_new()?;
    let expected_info = repository.info();
    let mut store = repository.into_store();
    let actual_info = ObjectRepo::peek_info(&mut store)?;

    assert_eq!(actual_info, expected_info);
    Ok(())
}

#[test]
fn committed_changes_are_persisted() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let mut handle = repo.add_unmanaged();

    // Write some data to the repository.
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();
    let expected_data = random_buffer();
    object.write_all(expected_data.as_slice())?;
    object.flush()?;

    drop(object);
    repo.commit()?;

    // Re-open the repository.
    let repo: ObjectRepo<_> = OpenOptions::new(repo.into_store()).open()?;

    // Read that data from the repository.
    let mut actual_data = Vec::with_capacity(expected_data.len());
    let mut object = repo.unmanaged_object(&handle).unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test]
fn uncommitted_changes_are_not_persisted() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let mut handle = repo.add_unmanaged();

    // Write some data to the repository.
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();
    let expected_data = random_buffer();
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);

    // Re-open the repository.
    let repo: ObjectRepo<_> = OpenOptions::new(repo.into_store()).open()?;

    assert!(repo.unmanaged_object(&handle).is_none());

    Ok(())
}

#[test]
fn unused_data_is_reclaimed_on_commit() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let mut handle = repo.add_unmanaged();
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);
    repo.commit()?;

    let mut store = repo.into_repo()?.into_store();
    let original_blocks = store.list_blocks()?.len();

    let mut repo = OpenOptions::new(store).open::<ObjectRepo<_>>()?;
    repo.remove_unmanaged(&handle);
    repo.commit()?;

    let mut store = repo.into_store();
    let new_blocks = store.list_blocks()?.len();

    assert!(new_blocks < original_blocks);
    Ok(())
}

#[test]
fn verify_valid_repository_is_valid() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let mut handle = repo.add_unmanaged();
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let report = repo.verify()?;

    assert!(!report.is_corrupt());
    Ok(())
}
