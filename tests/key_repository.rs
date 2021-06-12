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

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{peek_info, Encryption, OpenMode, OpenOptions, RepoConfig, SwitchInstance};
use acid_store::store::{DataStore, MemoryConfig, OpenStore};
use common::{assert_contains_all, random_buffer};

mod common;

fn create_repo(
    repo_config: RepoConfig,
    store_config: &MemoryConfig,
) -> acid_store::Result<KeyRepo<String>> {
    OpenOptions::new()
        .config(repo_config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(store_config)
}

fn open_repo(
    repo_config: RepoConfig,
    store_config: &MemoryConfig,
) -> acid_store::Result<KeyRepo<String>> {
    OpenOptions::new()
        .config(repo_config)
        .password(b"Password")
        .mode(OpenMode::Open)
        .open(store_config)
}

#[test]
fn opening_with_wrong_key_type_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    repo.insert(String::from("Test"));
    repo.commit()?;
    drop(repo);

    let repo_result: Result<KeyRepo<isize>, _> =
        OpenOptions::new().mode(OpenMode::Open).open(&store_config);

    assert!(matches!(repo_result, Err(acid_store::Error::Deserialize)));
    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn contains_key(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    repo.insert(String::from("test"));

    assert!(repo.contains("test"));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn remove_key(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    repo.insert(String::from("test"));

    assert!(repo.remove("test"));
    assert!(!repo.contains("test"));
    assert!(!repo.remove("test"));

    Ok(())
}

#[test]
fn list_keys() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(RepoConfig::default(), &store_config)?;

    repo.insert(String::from("test1"));
    repo.insert(String::from("test2"));
    repo.insert(String::from("test3"));

    let expected = vec![
        String::from("test1"),
        String::from("test2"),
        String::from("test3"),
    ];

    let actual = repo.keys().cloned().collect::<Vec<_>>();

    assert_contains_all(actual, expected);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn can_not_get_object_from_removed_key(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    repo.insert(String::from("test"));
    repo.remove("test");

    assert!(repo.object("test").is_none());
    assert!(repo.object_mut("test").is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn removing_copy_does_not_affect_original(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    repo.insert(String::from("original"));
    repo.copy("original", String::from("copy"));

    assert!(repo.remove("copy"));
    assert!(repo.contains("original"));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn copy_has_same_contents(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut object = repo.insert(String::from("original"));

    object.write_all(b"Data")?;
    object.flush()?;
    drop(object);

    assert!(repo.copy("original", String::from("copy")));

    let mut object = repo.object("copy").unwrap();
    let mut contents = Vec::new();
    object.read_to_end(&mut contents)?;
    drop(object);

    assert_eq!(contents, b"Data");

    Ok(())
}

#[test]
fn copied_object_must_exist() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(RepoConfig::default(), &store_config)?;

    assert!(!repo.copy("nonexistent", String::from("copy")));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn copying_overwrites_destination(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;

    let expected_data = random_buffer();

    let mut object = repo.insert(String::from("original"));
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let mut object = repo.insert(String::from("destination"));
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);

    assert!(repo.copy("original", String::from("destination")));

    let mut object = repo.object("destination").unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_ne!(actual_data, expected_data);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn existing_key_is_replaced(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(b"Data")?;
    object.flush()?;
    drop(object);

    let object = repo.insert(String::from("test"));
    assert_eq!(object.size(), 0);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn copy_nonexistent_object(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;

    assert!(!repo.copy("nonexistent1", String::from("nonexistent2")));

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
fn object_is_not_accessible_from_another_instance(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    repo.insert(String::from("test"));

    assert!(repo.contains("test"));
    assert!(repo.object("test").is_some());

    let repo: KeyRepo<String> = repo.switch_instance(Uuid::new_v4())?;

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

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

    let store_config = MemoryConfig::new();
    let repo = create_repo(repo_config.clone(), &store_config)?;

    let mut repo: KeyRepo<String> = repo.switch_instance(instance_1)?;
    repo.insert(String::from("test1"));

    let mut repo: KeyRepo<String> = repo.switch_instance(instance_2)?;
    repo.insert(String::from("test2"));

    repo.commit()?;
    drop(repo);
    let repo = open_repo(repo_config, &store_config)?;

    let repo: KeyRepo<String> = repo.switch_instance(instance_1)?;
    assert!(repo.contains("test1"));

    let repo: KeyRepo<String> = repo.switch_instance(instance_2)?;
    assert!(repo.contains("test2"));

    Ok(())
}

#[test]
fn change_password() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    repo.change_password(b"New password");
    repo.commit()?;
    drop(repo);

    OpenOptions::new()
        .password(b"New password")
        .open::<KeyRepo<String>, _>(&store_config)?;

    Ok(())
}

#[test]
fn peek_info_succeeds() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let repository: KeyRepo<String> = OpenOptions::new()
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
    let mut repo = create_repo(repo_config.clone(), &store_config)?;
    let mut object = repo.insert(String::from("test"));

    // Write some data to the repository.
    let expected_data = random_buffer();
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);

    repo.commit()?;
    drop(repo);

    // Re-open the repository.
    let repo = open_repo(repo_config, &store_config)?;

    // Read that data from the repository.
    let mut actual_data = Vec::with_capacity(expected_data.len());
    let mut object = repo.object("test").unwrap();
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
    let mut repo = create_repo(repo_config.clone(), &store_config)?;
    let mut object = repo.insert(String::from("test"));

    // Write some data to the repository.
    let expected_data = random_buffer();
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);
    drop(repo);

    // Re-open the repository.
    let repo = open_repo(repo_config, &store_config)?;

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn objects_are_removed_on_rollback(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.rollback()?;

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn object_contents_are_modified_on_rollback(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    repo.insert(String::from("test"));

    repo.commit()?;

    let mut object = repo.object_mut("test").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.rollback()?;

    let mut object = repo.object("test").unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(object.size(), 0);
    assert!(actual_data.is_empty());

    Ok(())
}

#[test]
fn rollback_before_first_commit_succeeds() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(RepoConfig::default(), &store_config)?;
    repo.commit()?;
    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn objects_are_removed_on_restore(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;

    let savepoint = repo.savepoint()?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let restore = repo.start_restore(&savepoint)?;

    assert!(repo.finish_restore(restore));

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn object_contents_are_modified_on_restore(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    repo.insert(String::from("test"));

    let savepoint = repo.savepoint()?;

    let mut object = repo.object_mut("test").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let restore = repo.start_restore(&savepoint)?;

    assert!(repo.finish_restore(restore));

    let mut object = repo.object("test").unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(object.size(), 0);
    assert!(actual_data.is_empty());

    Ok(())
}

#[test]
fn restore_can_redo_changes() -> anyhow::Result<()> {
    let mut repo = create_repo(RepoConfig::default(), &MemoryConfig::new())?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let before_savepoint = repo.savepoint()?;

    repo.remove("test");

    let after_savepoint = repo.savepoint()?;

    let restore = repo.start_restore(&before_savepoint)?;
    repo.finish_restore(restore);

    assert!(repo.contains("test"));
    assert!(repo.object("test").is_some());

    let restore = repo.start_restore(&after_savepoint)?;
    repo.finish_restore(restore);

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test]
fn committing_repo_invalidates_savepoint() -> anyhow::Result<()> {
    let mut repo = create_repo(RepoConfig::default(), &MemoryConfig::new())?;

    let before_savepoint = repo.savepoint()?;
    repo.commit()?;

    assert!(!before_savepoint.is_valid());
    assert!(matches!(
        repo.start_restore(&before_savepoint),
        Err(acid_store::Error::InvalidSavepoint)
    ));

    let after_savepoint = repo.savepoint()?;

    assert!(after_savepoint.is_valid());
    assert!(repo.start_restore(&after_savepoint).is_ok());

    Ok(())
}

#[test]
fn dropping_repo_invalidates_savepoint() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(RepoConfig::default(), &store_config)?;

    let savepoint = repo.savepoint()?;
    drop(repo);

    assert!(!savepoint.is_valid());

    Ok(())
}

#[test]
fn savepoint_must_be_associated_with_repo() -> anyhow::Result<()> {
    let mut first_repo = create_repo(RepoConfig::default(), &MemoryConfig::new())?;
    let mut second_repo = create_repo(RepoConfig::default(), &MemoryConfig::new())?;

    let savepoint = first_repo.savepoint()?;

    assert!(savepoint.is_valid());
    assert!(matches!(
        second_repo.start_restore(&savepoint),
        Err(acid_store::Error::InvalidSavepoint)
    ));

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
    let mut repo = create_repo(repo_config.clone(), &store_config)?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);
    repo.commit()?;
    drop(repo);

    let mut store = store_config.open()?;
    let original_blocks = store.list_blocks()?.len();
    drop(store);

    let mut repo = open_repo(repo_config, &store_config)?;
    repo.remove("test");
    repo.commit()?;
    repo.clean()?;
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
    let mut object = repo.insert(String::from("test"));

    let expected_data = random_buffer();
    let mut actual_data = Vec::new();

    // Write to an object and commit.
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    drop(object);
    repo.commit()?;

    // Delete that object, clean without committing first, and then roll back.
    repo.remove("test");
    repo.clean()?;
    repo.rollback()?;

    // Check if the object still exists.
    assert!(repo.contains("test"));

    // Check if the object's data was cleaned up.
    let mut object = repo.object("test").unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data.as_slice(), expected_data.as_slice());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn clear_instance_deletes_objects(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.clear_instance();

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn clear_repo_deletes_objects(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.clear_repo();

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn rollback_after_clear_instance(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert!(repo.contains("test"));
    assert!(repo.object("test").is_some());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn rollback_after_clear_repo(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    repo.commit()?;
    repo.clear_repo();
    repo.rollback()?;

    assert!(repo.contains("test"));
    assert!(repo.object("test").is_some());

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
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    assert!(repo.verify()?.is_empty());

    Ok(())
}
