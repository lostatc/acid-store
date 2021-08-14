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

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{
    peek_info, Commit, Encryption, ResourceLimit, RestoreSavepoint, SwitchInstance, Unlock,
};
use acid_store::store::{BlockType, DataStore, OpenStore};
use common::*;
use rstest_reuse::{self, *};
use std::collections::HashSet;
use uuid::Uuid;

mod common;

#[rstest]
fn opening_with_wrong_key_type_errs(repo_store: RepoStore) -> anyhow::Result<()> {
    let mut repo: KeyRepo<String> = repo_store.create()?;
    repo.insert(String::from("Test"));
    repo.commit()?;
    drop(repo);

    assert_that!(repo_store.open::<KeyRepo<isize>>())
        .is_err_variant(acid_store::Error::Deserialize);

    Ok(())
}

#[rstest]
fn contains_key(mut repo: KeyRepo<String>) {
    repo.insert(String::from("test"));
    assert_that!(repo.contains("test")).is_true();
}

#[rstest]
fn remove_key(mut repo: KeyRepo<String>) {
    repo.insert(String::from("test"));

    assert_that!(repo.remove("test")).is_true();
    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.remove("test")).is_false();
}

#[rstest]
fn list_keys(mut repo: KeyRepo<String>) {
    repo.insert(String::from("test1"));
    repo.insert(String::from("test2"));
    repo.insert(String::from("test3"));

    assert_that!(repo.keys().cloned().collect::<Vec<_>>()).contains_all_of(&[
        &String::from("test1"),
        &String::from("test2"),
        &String::from("test3"),
    ]);
}

#[rstest]
fn can_not_get_object_from_removed_key(mut repo: KeyRepo<String>) {
    repo.insert(String::from("test"));
    repo.remove("test");

    assert_that!(repo.object("test")).is_none();
}

#[rstest]
fn removing_copy_does_not_affect_original(mut repo: KeyRepo<String>) {
    repo.insert(String::from("original"));
    repo.copy("original", String::from("copy"));

    assert_that!(repo.remove("copy")).is_true();
    assert_that!(repo.contains("original")).is_true();
}

#[apply(object_config)]
fn copy_has_same_contents(#[case] repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    assert_that!(repo.copy(&key, String::from("copy"))).is_true();

    let mut object = repo.object("copy").unwrap();
    let mut actual_contents = Vec::new();
    object.read_to_end(&mut actual_contents)?;
    drop(object);

    assert_that!(actual_contents).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
fn copied_object_must_exist(mut repo: KeyRepo<String>) {
    assert_that!(repo.copy("nonexistent", String::from("copy"))).is_false();
}

#[apply(object_config)]
fn copying_overwrites_destination(
    #[case] repo_object: RepoObject,
    #[from(buffer)] junk_buffer: Vec<u8>,
    #[from(buffer)] expected_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(&expected_buffer)?;
    object.commit()?;
    drop(object);

    let mut object = repo.insert(String::from("destination"));
    object.write_all(&junk_buffer)?;
    object.commit()?;
    drop(object);

    assert_that!(repo.copy(&key, String::from("destination"))).is_true();

    let mut object = repo.object("destination").unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_that!(actual_data).is_equal_to(&expected_buffer);

    Ok(())
}

#[rstest]
fn existing_key_is_replaced(repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    let object = repo.insert(key);

    assert_that!(object.size()).is_ok_containing(0);

    Ok(())
}

#[rstest]
fn copy_nonexistent_object(mut repo: KeyRepo<String>) {
    assert_that!(repo.copy("nonexistent1", String::from("nonexistent2"))).is_false();
}

#[rstest]
fn object_is_not_accessible_from_another_instance(repo_object: RepoObject) -> anyhow::Result<()> {
    let RepoObject { repo, key, .. } = repo_object;

    assert_that!(repo.contains(&key)).is_true();
    assert_that!(repo.object(&key)).is_some();

    let repo: KeyRepo<String> = repo.switch_instance(Uuid::new_v4().into())?;

    assert_that!(repo.contains(&key)).is_false();
    assert_that!(repo.object(&key)).is_none();

    Ok(())
}

#[rstest]
fn committing_commits_all_instances(repo_store: RepoStore) -> anyhow::Result<()> {
    let instance_1 = Uuid::new_v4().into();
    let instance_2 = Uuid::new_v4().into();

    let repo: KeyRepo<String> = repo_store.create()?;

    let mut repo: KeyRepo<String> = repo.switch_instance(instance_1)?;
    repo.insert(String::from("test1"));

    let mut repo: KeyRepo<String> = repo.switch_instance(instance_2)?;
    repo.insert(String::from("test2"));

    repo.commit()?;
    drop(repo);
    let repo: KeyRepo<String> = repo_store.open()?;

    let repo: KeyRepo<String> = repo.switch_instance(instance_1)?;
    assert_that!(repo.contains("test1")).is_true();

    let repo: KeyRepo<String> = repo.switch_instance(instance_2)?;
    assert_that!(repo.contains("test2")).is_true();

    Ok(())
}

#[rstest]
fn change_password(mut repo_store: RepoStore) -> anyhow::Result<()> {
    repo_store.config.encryption = Encryption::XChaCha20Poly1305;
    let mut repo: KeyRepo<String> = repo_store.create()?;

    repo.change_password(
        b"New password",
        ResourceLimit::Interactive,
        ResourceLimit::Interactive,
    );
    repo.commit()?;
    drop(repo);

    repo_store.password = String::from("New password");

    assert_that!(repo_store.open::<KeyRepo<String>>()).is_ok();

    Ok(())
}

#[rstest]
fn peek_info_succeeds(repo_store: RepoStore) -> anyhow::Result<()> {
    let repo: KeyRepo<String> = repo_store.create()?;
    let expected_info = repo.info();
    let actual_info = peek_info(&repo_store.store)?;

    assert_that!(actual_info).is_equal_to(expected_info);

    Ok(())
}

#[apply(store_config)]
fn committed_changes_are_persisted(
    #[case] repo_store: RepoStore,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut repo: KeyRepo<String> = repo_store.create()?;
    let mut object = repo.insert(String::from("test"));

    // Write some data to the repository.
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    repo.commit()?;
    drop(repo);

    // Re-open the repository.
    let repo: KeyRepo<String> = repo_store.open()?;

    // Read that data from the repository.
    let mut actual_data = Vec::new();
    let mut object = repo.object("test").unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_that!(actual_data).is_equal_to(&buffer);

    Ok(())
}

#[apply(store_config)]
fn uncommitted_changes_are_not_persisted(
    #[case] repo_store: RepoStore,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut repo: KeyRepo<String> = repo_store.create()?;
    let mut object = repo.insert(String::from("test"));

    // Write some data to the repository.
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);
    drop(repo);

    // Re-open the repository.
    let repo: KeyRepo<String> = repo_store.open()?;

    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.object("test")).is_none();

    Ok(())
}

#[rstest]
fn objects_are_removed_on_rollback(repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    repo.rollback()?;

    assert_that!(repo.contains(&key)).is_false();
    assert_that!(repo.object(&key)).is_none();

    Ok(())
}

#[apply(object_config)]
fn object_contents_are_modified_on_rollback(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    repo.commit()?;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    repo.rollback()?;

    let mut actual_data = Vec::new();
    let mut object = repo.object(&key).unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_that!(object.size()).is_ok_containing(0);
    assert_that!(actual_data).is_empty();

    Ok(())
}

#[rstest]
fn rollback_before_first_commit_succeeds(mut repo: KeyRepo<String>) {
    assert_that!(repo.rollback()).is_ok();
}

#[rstest]
fn objects_are_removed_on_restore(mut repo: KeyRepo<String>) -> anyhow::Result<()> {
    let savepoint = repo.savepoint()?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    repo.restore(&savepoint)?;

    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.object("test")).is_none();

    Ok(())
}

#[apply(object_config)]
fn object_contents_are_modified_on_restore(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    let savepoint = repo.savepoint()?;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    assert_that!(repo.restore(&savepoint)).is_ok();

    let mut actual_data = Vec::new();
    let mut object = repo.object(&key).unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_that!(object.size()).is_ok_containing(0);
    assert_that!(actual_data).is_empty();

    Ok(())
}

#[rstest]
fn restore_can_redo_changes(repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    let before_savepoint = repo.savepoint()?;

    repo.remove(&key);

    let after_savepoint = repo.savepoint()?;

    assert_that!(repo.restore(&before_savepoint)).is_ok();
    assert_that!(repo.contains(&key)).is_true();
    assert_that!(repo.object(&key)).is_some();

    assert_that!(repo.restore(&after_savepoint)).is_ok();
    assert_that!(repo.contains(&key)).is_false();
    assert_that!(repo.object(&key)).is_none();

    Ok(())
}

#[rstest]
fn committing_repo_invalidates_savepoint(mut repo: KeyRepo<String>) -> anyhow::Result<()> {
    let before_savepoint = repo.savepoint()?;
    repo.commit()?;

    assert_that!(before_savepoint.is_valid()).is_false();
    assert_that!(repo.start_restore(&before_savepoint))
        .is_err_variant(acid_store::Error::InvalidSavepoint);

    let after_savepoint = repo.savepoint()?;

    assert_that!(after_savepoint.is_valid()).is_true();
    assert_that!(repo.start_restore(&after_savepoint)).is_ok();

    Ok(())
}

#[rstest]
fn dropping_repo_invalidates_savepoint(mut repo: KeyRepo<String>) -> anyhow::Result<()> {
    let savepoint = repo.savepoint()?;
    drop(repo);

    assert_that!(savepoint.is_valid()).is_false();

    Ok(())
}

#[rstest]
fn savepoint_must_be_associated_with_repo(
    #[from(repo)] mut first_repo: KeyRepo<String>,
    #[from(repo)] mut second_repo: KeyRepo<String>,
) -> anyhow::Result<()> {
    let savepoint = first_repo.savepoint()?;

    assert_that!(savepoint.is_valid()).is_true();
    assert_that!(second_repo.start_restore(&savepoint))
        .is_err_variant(acid_store::Error::InvalidSavepoint);

    Ok(())
}

#[apply(store_config)]
fn unused_data_is_reclaimed_on_clean(
    #[case] repo_store: RepoStore,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut repo: KeyRepo<String> = repo_store.create()?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);
    repo.commit()?;
    drop(repo);

    let mut store = repo_store.store.open()?;
    let original_blocks = store.list_blocks(BlockType::Data)?.len();
    drop(store);

    let mut repo: KeyRepo<String> = repo_store.open()?;
    repo.remove("test");
    repo.commit()?;
    repo.clean()?;
    drop(repo);

    let mut store = repo_store.store.open()?;
    let new_blocks = store.list_blocks(BlockType::Data)?.len();

    assert_that!(new_blocks).is_less_than(original_blocks);

    Ok(())
}

#[apply(object_config)]
fn clean_before_commit_does_not_prevent_rollback(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    // Write to an object and commit.
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);
    repo.commit()?;

    // Delete that object, clean without committing first, and then roll back.
    repo.remove(&key);
    repo.clean()?;

    assert_that!(repo.rollback()).is_ok();

    // Check if the object still exists.
    assert_that!(repo.contains(&key)).is_true();

    // Check if the object's data was cleaned up.
    let mut actual_data = Vec::new();
    let mut object = repo.object(&key).unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_that!(actual_data).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
fn clear_instance_deletes_objects(repo_object: RepoObject) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    repo.clear_instance();

    assert_that!(repo.contains(&key)).is_false();
    assert_that!(repo.object("test")).is_none();

    Ok(())
}

#[rstest]
fn rollback_after_clear_instance(repo_object: RepoObject) -> anyhow::Result<()> {
    let RepoObject {
        mut repo,
        mut object,
        key,
    } = repo_object;

    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert_that!(repo.contains(&key)).is_true();
    assert_that!(repo.object(&key)).is_some();

    Ok(())
}

#[apply(object_config)]
fn verify_valid_repository_is_valid(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let RepoObject {
        repo, mut object, ..
    } = repo_object;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    assert_that!(repo.verify()).is_ok_containing(HashSet::new());

    Ok(())
}

#[rstest]
fn actual_and_apparent_size_are_correct(
    repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let RepoObject {
        mut object,
        mut repo,
        ..
    } = repo_object;

    let hole_size = 100;

    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    let mut object = repo.insert(String::from("test"));
    object.write_all(&buffer)?;
    object.commit()?;
    object.set_len(buffer.len() as u64 + hole_size)?;
    drop(object);

    let stats = repo.stats();

    assert_that!(stats.apparent_size()).is_equal_to((buffer.len() as u64 * 2) + hole_size);
    assert_that!(stats.actual_size()).is_equal_to(buffer.len() as u64);

    Ok(())
}

#[rstest]
fn actual_and_apparent_size_are_for_current_instance(
    repo_object: RepoObject,
    #[from(buffer)] current_buffer: Vec<u8>,
    #[from(buffer)] other_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let RepoObject {
        mut object, repo, ..
    } = repo_object;

    let instance_id = Uuid::new_v4().into();

    object.write_all(&other_buffer)?;
    object.commit()?;
    drop(object);

    let mut repo: KeyRepo<String> = repo.switch_instance(instance_id)?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(&current_buffer)?;
    object.commit()?;
    drop(object);

    let stats = repo.stats();

    assert_that!(stats.apparent_size()).is_equal_to(current_buffer.len() as u64);
    assert_that!(stats.actual_size()).is_equal_to(current_buffer.len() as u64);

    Ok(())
}

#[rstest]
fn repo_size_is_correct(
    repo_object: RepoObject,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let RepoObject {
        mut object, repo, ..
    } = repo_object;

    let instance_id = Uuid::new_v4().into();

    object.write_all(&first_buffer)?;
    object.commit()?;
    drop(object);

    let mut repo: KeyRepo<String> = repo.switch_instance(instance_id)?;

    let mut object = repo.insert(String::from("test1"));
    object.write_all(&first_buffer)?;
    object.commit()?;
    drop(object);

    let mut object = repo.insert(String::from("test2"));
    object.write_all(&second_buffer)?;
    object.commit()?;
    drop(object);

    let stats = repo.stats();

    assert_that!(stats.repo_size())
        .is_equal_to(first_buffer.len() as u64 + second_buffer.len() as u64);

    Ok(())
}

#[rstest]
fn unlock_repo(repo_store: RepoStore) -> anyhow::Result<()> {
    let repo: KeyRepo<String> = repo_store.create()?;
    repo.unlock()?;
    assert_that!(repo_store.open::<KeyRepo<String>>()).is_ok();
    Ok(())
}

#[rstest]
fn check_repo_is_locked(repo_store: RepoStore) -> anyhow::Result<()> {
    let repo: KeyRepo<String> = repo_store.create()?;
    assert_that!(repo.is_locked()).is_ok_containing(true);
    repo.unlock()?;
    assert_that!(repo.is_locked()).is_ok_containing(false);
    Ok(())
}

#[rstest]
fn get_lock_context(mut repo_store: RepoStore) -> anyhow::Result<()> {
    repo_store.context = b"lock context value".to_vec();
    let repo: KeyRepo<String> = repo_store.create()?;
    assert_that!(repo.context()).is_ok_containing(&repo_store.context);
    Ok(())
}

#[rstest]
fn update_lock_context(mut repo_store: RepoStore) -> anyhow::Result<()> {
    repo_store.context = b"initial context".to_vec();
    let repo: KeyRepo<String> = repo_store.create()?;
    repo.update_context(b"updated context")?;
    repo_store.handler = Box::new(|context| {
        assert_that!(context).is_equal_to(&b"updated context"[..]);
        true
    });
    assert_that!(repo_store.open::<KeyRepo<String>>()).is_ok();
    Ok(())
}
