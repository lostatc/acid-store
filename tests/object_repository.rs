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

use uuid::Uuid;

use acid_store::repo::object::ObjectRepo;
use acid_store::repo::{ConvertRepo, Encryption, OpenOptions};
use acid_store::store::{DataStore, MemoryStore};
use common::{assert_contains_all, random_buffer};

mod common;

fn create_repo() -> acid_store::Result<ObjectRepo<MemoryStore>> {
    OpenOptions::new(MemoryStore::new()).create_new()
}

#[test]
fn contains_unmanaged_object() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let handle = repo.add_unmanaged();

    assert!(repo.contains_unmanaged(&handle));

    Ok(())
}

#[test]
fn remove_unmanaged_object() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let handle = repo.add_unmanaged();

    assert!(repo.remove_unmanaged(&handle));
    assert!(!repo.contains_unmanaged(&handle));
    assert!(!repo.remove_unmanaged(&handle));

    Ok(())
}

#[test]
fn can_not_get_object_from_removed_handle() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let mut handle = repo.add_unmanaged();
    repo.remove_unmanaged(&handle);

    assert!(repo.unmanaged_object(&handle).is_none());
    assert!(repo.unmanaged_object_mut(&mut handle).is_none());

    Ok(())
}

#[test]
fn removing_unmanaged_copy_does_not_affect_original() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let handle = repo.add_unmanaged();
    let copy = repo.copy_unmanaged(&handle);

    assert!(repo.remove_unmanaged(&copy));
    assert!(repo.contains_unmanaged(&handle));

    Ok(())
}

#[test]
fn unmanaged_copy_has_same_contents() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let mut handle = repo.add_unmanaged();

    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();
    object.write_all(b"Data")?;
    object.flush()?;
    drop(object);

    let copy = repo.copy_unmanaged(&handle);

    let mut object = repo.unmanaged_object(&copy).unwrap();
    let mut contents = Vec::new();
    object.read_to_end(&mut contents)?;
    drop(object);

    assert_eq!(contents, b"Data");

    Ok(())
}

#[test]
fn contains_managed_object() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let id = Uuid::new_v4();
    repo.add_managed(id);

    assert!(repo.contains_managed(id));

    Ok(())
}

#[test]
fn remove_managed_object() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let id = Uuid::new_v4();
    repo.add_managed(id);

    assert!(repo.remove_managed(id));
    assert!(!repo.contains_managed(id));
    assert!(!repo.remove_managed(id));

    Ok(())
}

#[test]
fn managed_object_is_replaced() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let id = Uuid::new_v4();

    let mut object = repo.add_managed(id);
    object.write_all(b"Data")?;
    object.flush()?;
    drop(object);

    let object = repo.add_managed(id);
    assert_eq!(object.size(), 0);

    Ok(())
}

#[test]
fn copy_nonexistent_managed_object() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    assert!(!repo.copy_managed(Uuid::new_v4(), Uuid::new_v4()));
    Ok(())
}

#[test]
fn managed_copy_has_same_contents() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let source_id = Uuid::new_v4();
    let dest_id = Uuid::new_v4();

    let mut object = repo.add_managed(source_id);
    object.write_all(b"Data")?;
    object.flush()?;
    drop(object);

    assert!(repo.copy_managed(source_id, dest_id));

    let mut object = repo.managed_object(dest_id).unwrap();
    let mut contents = Vec::new();
    object.read_to_end(&mut contents)?;
    drop(object);

    assert_eq!(contents, b"Data");

    Ok(())
}

#[test]
fn list_managed_objects() -> anyhow::Result<()> {
    let mut repo = create_repo()?;

    let id_1 = Uuid::new_v4();
    let id_2 = Uuid::new_v4();
    let id_3 = Uuid::new_v4();

    repo.add_managed(id_1);
    repo.add_managed(id_2);
    repo.add_managed(id_3);

    let expected = vec![id_1, id_2, id_3];
    let actual = repo.list_managed();

    assert_contains_all(actual, expected);

    Ok(())
}

#[test]
fn unmanaged_object_is_not_accessible_from_another_instance() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let handle = repo.add_unmanaged();

    assert!(repo.unmanaged_object(&handle).is_some());

    repo.set_instance(Uuid::new_v4());

    assert!(repo.unmanaged_object(&handle).is_none());

    Ok(())
}

#[test]
fn managed_object_is_not_accessible_from_another_instance() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let id = Uuid::new_v4();

    repo.add_managed(id);

    assert!(repo.managed_object(id).is_some());

    repo.set_instance(Uuid::new_v4());

    assert!(repo.managed_object(id).is_none());

    Ok(())
}

#[test]
fn list_managed_objects_in_different_instance() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    let instance_id = repo.instance();

    let id_1 = Uuid::new_v4();
    let id_2 = Uuid::new_v4();
    repo.add_managed(id_1);
    repo.add_managed(id_2);

    repo.set_instance(Uuid::new_v4());

    let id_3 = Uuid::new_v4();
    repo.add_managed(id_3);

    let expected = vec![id_3];
    let actual = repo.list_managed();
    assert_contains_all(actual, expected);

    repo.set_instance(instance_id);

    let expected = vec![id_1, id_2];
    let actual = repo.list_managed();
    assert_contains_all(actual, expected);

    Ok(())
}

#[test]
fn committing_commits_all_instances() -> anyhow::Result<()> {
    let instance_1 = Uuid::new_v4();
    let instance_2 = Uuid::new_v4();
    let id_1 = Uuid::new_v4();
    let id_2 = Uuid::new_v4();

    let mut repo = create_repo()?;

    repo.set_instance(instance_1);
    repo.add_managed(id_1);

    repo.set_instance(instance_2);
    repo.add_managed(id_2);

    repo.commit()?;
    let mut repo = OpenOptions::new(repo.into_store()).open::<ObjectRepo<_>>()?;

    repo.set_instance(instance_1);
    assert!(repo.contains_managed(id_1));

    repo.set_instance(instance_2);
    assert!(repo.contains_managed(id_2));

    Ok(())
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
