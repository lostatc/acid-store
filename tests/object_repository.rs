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

use test_case::test_case;
use uuid::Uuid;

use acid_store::repo::object::ObjectRepo;
use acid_store::repo::{peek_info, Encryption, OpenMode, OpenOptions, RepoConfig};
use acid_store::store::{DataStore, MemoryConfig, OpenStore};
use common::{assert_contains_all, random_buffer};

mod common;

fn create_repo(
    repo_config: RepoConfig,
    store_config: &MemoryConfig,
) -> acid_store::Result<ObjectRepo> {
    OpenOptions::new()
        .config(repo_config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(store_config)
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn contains_unmanaged_object(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let handle = repo.add_unmanaged();

    assert!(repo.contains_unmanaged(&handle));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn remove_unmanaged_object(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let handle = repo.add_unmanaged();

    assert!(repo.remove_unmanaged(&handle));
    assert!(!repo.contains_unmanaged(&handle));
    assert!(!repo.remove_unmanaged(&handle));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn can_not_get_object_from_removed_handle(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut handle = repo.add_unmanaged();
    repo.remove_unmanaged(&handle);

    assert!(repo.unmanaged_object(&handle).is_none());
    assert!(repo.unmanaged_object_mut(&mut handle).is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn removing_unmanaged_copy_does_not_affect_original(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let handle = repo.add_unmanaged();
    let copy = repo.copy_unmanaged(&handle);

    assert!(repo.remove_unmanaged(&copy));
    assert!(repo.contains_unmanaged(&handle));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn unmanaged_copy_has_same_contents(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
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

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn contains_managed_object(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let id = Uuid::new_v4();
    repo.add_managed(id);

    assert!(repo.contains_managed(id));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn remove_managed_object(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let id = Uuid::new_v4();
    repo.add_managed(id);

    assert!(repo.remove_managed(id));
    assert!(!repo.contains_managed(id));
    assert!(!repo.remove_managed(id));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn managed_object_is_replaced(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let id = Uuid::new_v4();

    let mut object = repo.add_managed(id);
    object.write_all(b"Data")?;
    object.flush()?;
    drop(object);

    let object = repo.add_managed(id);
    assert_eq!(object.size(), 0);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn copy_nonexistent_managed_object(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    assert!(!repo.copy_managed(Uuid::new_v4(), Uuid::new_v4()));
    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn managed_copy_has_same_contents(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
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

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn list_managed_objects(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;

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

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn unmanaged_object_is_not_accessible_from_another_instance(
    repo_config: RepoConfig,
) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let handle = repo.add_unmanaged();

    assert!(repo.unmanaged_object(&handle).is_some());

    repo.set_instance(Uuid::new_v4());

    assert!(repo.unmanaged_object(&handle).is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn managed_object_is_not_accessible_from_another_instance(
    repo_config: RepoConfig,
) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let id = Uuid::new_v4();

    repo.add_managed(id);

    assert!(repo.managed_object(id).is_some());

    repo.set_instance(Uuid::new_v4());

    assert!(repo.managed_object(id).is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn list_managed_objects_in_different_instance(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
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

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn committing_commits_all_instances(repo_config: RepoConfig) -> anyhow::Result<()> {
    let instance_1 = Uuid::new_v4();
    let instance_2 = Uuid::new_v4();
    let id_1 = Uuid::new_v4();
    let id_2 = Uuid::new_v4();

    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;

    repo.set_instance(instance_1);
    repo.add_managed(id_1);

    repo.set_instance(instance_2);
    repo.add_managed(id_2);

    repo.commit()?;
    let mut repo: ObjectRepo = OpenOptions::new().open(&store_config)?;

    repo.set_instance(instance_1);
    assert!(repo.contains_managed(id_1));

    repo.set_instance(instance_2);
    assert!(repo.contains_managed(id_2));

    Ok(())
}

#[test]
fn change_password() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: ObjectRepo = OpenOptions::new()
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    repo.change_password(b"New password");
    repo.commit()?;
    drop(repo);

    OpenOptions::new()
        .password(b"New password")
        .open::<ObjectRepo, _>(&store_config)?;

    Ok(())
}

#[test]
fn peek_info_succeeds() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let repository: ObjectRepo = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let expected_info = repository.info();
    let actual_info = peek_info(&store_config)?;

    assert_eq!(actual_info, expected_info);
    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn committed_changes_are_persisted(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut handle = repo.add_unmanaged();

    // Write some data to the repository.
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();
    let expected_data = random_buffer();
    object.write_all(expected_data.as_slice())?;
    object.flush()?;

    drop(object);
    repo.commit()?;
    drop(repo);

    // Re-open the repository.
    let repo: ObjectRepo = OpenOptions::new().open(&store_config)?;

    // Read that data from the repository.
    let mut actual_data = Vec::with_capacity(expected_data.len());
    let mut object = repo.unmanaged_object(&handle).unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn uncommitted_changes_are_not_persisted(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut handle = repo.add_unmanaged();

    // Write some data to the repository.
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();
    let expected_data = random_buffer();
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);
    drop(repo);

    // Re-open the repository.
    let repo: ObjectRepo = OpenOptions::new().open(&store_config)?;

    assert!(repo.unmanaged_object(&handle).is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn unmanaged_objects_are_removed_on_rollaback(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut handle = repo.add_unmanaged();

    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.rollback()?;

    assert!(!repo.contains_unmanaged(&handle));
    assert!(repo.unmanaged_object(&handle).is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn managed_objects_are_removed_on_rollaback(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let id = Uuid::new_v4();

    let mut object = repo.add_managed(id);
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.rollback()?;

    assert!(!repo.contains_managed(id));
    assert!(repo.managed_object(id).is_none());
    assert!(repo.list_managed().next().is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn unused_data_is_reclaimed_on_commit(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut handle = repo.add_unmanaged();
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);
    repo.commit()?;
    drop(repo);

    let mut store = store_config.open()?;
    let original_blocks = store.list_blocks()?.len();
    drop(store);

    let mut repo: ObjectRepo = OpenOptions::new().open(&store_config)?;
    repo.remove_unmanaged(&handle);
    repo.commit()?;
    drop(repo);

    let mut store = store_config.open()?;
    let new_blocks = store.list_blocks()?.len();

    assert!(new_blocks < original_blocks);
    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn clean_before_commit_does_not_prevent_rollback(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let id = Uuid::new_v4();
    let mut object = repo.add_managed(id);

    let expected_data = random_buffer();
    let mut actual_data = Vec::new();

    // Write to an object and commit.
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);
    repo.commit()?;

    // Delete that object, clean without committing first, and then roll back.
    repo.remove_managed(id);
    repo.clean()?;
    repo.rollback()?;

    // Check if the object still exists.
    assert!(repo.contains_managed(id));

    // Check if the object's data was cleaned up.
    let mut object = repo.managed_object(id).unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data.as_slice(), expected_data.as_slice());

    Ok(())
}

#[test]
fn clear_repo_deletes_managed_objects() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(RepoConfig::default(), &store_config)?;
    let object_id = Uuid::new_v4();

    let mut object = repo.add_managed(object_id);
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.clear_repo();

    assert!(!repo.contains_managed(object_id));
    assert!(repo.managed_object(object_id).is_none());

    Ok(())
}

#[test]
fn clear_repo_deletes_unmanaged_objects() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(RepoConfig::default(), &store_config)?;

    let mut handle = repo.add_unmanaged();
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.clear_repo();

    assert!(!repo.contains_unmanaged(&handle));
    assert!(repo.unmanaged_object(&handle).is_none());

    Ok(())
}

#[test]
fn rollback_after_clear_repo() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(RepoConfig::default(), &store_config)?;
    let object_id = Uuid::new_v4();

    let mut object = repo.add_managed(object_id);
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.commit()?;
    repo.clear_repo();
    repo.rollback()?;

    assert!(repo.contains_managed(object_id));
    assert!(repo.managed_object(object_id).is_some());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn verify_valid_repository_is_valid(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut handle = repo.add_unmanaged();
    let mut object = repo.unmanaged_object_mut(&mut handle).unwrap();

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let report = repo.verify()?;

    assert!(!report.is_corrupt());
    Ok(())
}
