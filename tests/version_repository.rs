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

#![cfg(all(
    feature = "repo-version",
    feature = "encryption",
    feature = "compression"
))]

use std::io::{Read, Write};
use std::iter::FromIterator;

use acid_store::repo::version::VersionRepo;
use acid_store::repo::{Commit, SwitchInstance, DEFAULT_INSTANCE};
use acid_store::uuid::Uuid;
use common::*;

mod common;

#[rstest]
fn switching_instance_does_not_roll_back(mut repo: VersionRepo<String>) -> anyhow::Result<()> {
    let mut object = repo.insert("test".to_string()).unwrap();
    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    let repo: VersionRepo<String> = repo.switch_instance(Uuid::new_v4().into())?;
    let repo: VersionRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;

    assert_that!(repo.contains("test")).is_true();
    assert_that!(repo.object("test")).is_some();

    Ok(())
}

#[rstest]
fn switching_instance_does_not_commit(mut repo: VersionRepo<String>) -> anyhow::Result<()> {
    let mut object = repo.insert("test".to_string()).unwrap();
    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    let repo: VersionRepo<String> = repo.switch_instance(Uuid::new_v4().into())?;
    let mut repo: VersionRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.object("test")).is_none();

    Ok(())
}

#[rstest]
fn read_version(mut repo: VersionRepo<String>, buffer: Vec<u8>) -> anyhow::Result<()> {
    // Add a new object and write data to it.
    let mut object = repo.insert(String::from("Key")).unwrap();
    object.write_all(&buffer)?;
    object.commit()?;
    drop(object);

    // Create a new version of the object.
    let version = repo.create_version("Key").unwrap();

    // Read the new version.
    let mut object = repo.version_object("Key", version.id()).unwrap();

    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_that!(actual_data).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
fn list_versions(mut repo: VersionRepo<String>) {
    repo.insert("Key".into()).unwrap();
    let version1 = repo.create_version("Key").unwrap();
    let version2 = repo.create_version("Key").unwrap();
    let version3 = repo.create_version("Key").unwrap();

    assert_that!(repo.versions("Key").map(Vec::from_iter))
        .is_some()
        .contains_all_of(&[&version1, &version2, &version3]);
}

#[rstest]
fn remove_version(mut repo: VersionRepo<String>) {
    repo.insert(String::from("Key")).unwrap();
    let version = repo.create_version("Key").unwrap();

    assert_that!(repo.remove_version("Nonexistent", version.id())).is_false();
    assert_that!(repo.remove_version("Key", version.id() + 1)).is_false();
    assert_that!(repo.remove_version("Key", version.id())).is_true();
    assert_that!(repo.version_object("Key", version.id())).is_none();
}

#[rstest]
fn remove_and_list_versions(mut repo: VersionRepo<String>) {
    repo.insert("Key".into()).unwrap();
    let version1 = repo.create_version("Key").unwrap();
    let version2 = repo.create_version("Key").unwrap();
    let version3 = repo.create_version("Key").unwrap();

    assert_that!(repo.remove_version("Key", version2.id())).is_true();

    assert_that!(repo.versions("Key").map(Vec::from_iter))
        .is_some()
        .contains_all_of(&[&version1, &version3])
}

#[rstest]
fn remove_and_get_version(mut repo: VersionRepo<String>) {
    repo.insert("Key".into()).unwrap();
    let version = repo.create_version("Key").unwrap();

    assert_that!(repo.get_version("Key", version.id())).contains_value(&version);
    assert_that!(repo.remove_version("Key", version.id())).is_true();
    assert_that!(repo.get_version("Key", version.id())).is_none();
}

#[rstest]
fn versioning_nonexistent_key_errs(mut repo: VersionRepo<String>) {
    assert_that!(repo.create_version("Key")).is_none();
    assert_that!(repo.remove_version("Key", 1)).is_false();
    assert_that!(repo.versions("Key").map(Vec::from_iter)).is_none();
}

#[rstest]
fn removing_key_removes_versions(mut repo: VersionRepo<String>) {
    repo.insert("Key".into()).unwrap();
    let version = repo.create_version("Key").unwrap();

    assert_that!(repo.remove("Key")).is_true();
    assert_that!(repo.version_object("Key", version.id())).is_none();
    assert_that!(repo.versions("Key").map(Vec::from_iter)).is_none();
    assert_that!(repo.get_version("Key", version.id())).is_none();
}

#[rstest]
fn restore_version(
    mut repo: VersionRepo<String>,
    #[from(buffer)] expected_buffer: Vec<u8>,
    #[from(buffer)] junk_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    // Create an object and write data to it.
    let mut object = repo.insert("Key".into()).unwrap();
    object.write_all(&expected_buffer)?;
    object.commit()?;
    drop(object);

    // Create a new version.
    let version = repo.create_version("Key").unwrap();

    // Modify the contents of the object.
    let mut object = repo.object("Key").unwrap();
    object.write_all(&junk_buffer)?;
    object.commit()?;
    drop(object);

    // Restore the contents from the version.
    assert_that!(repo.restore_version("Key", version.id())).is_true();

    // Check the contents.
    let mut actual_data = Vec::new();
    let mut object = repo.object("Key").unwrap();
    object.read_to_end(&mut actual_data)?;

    assert_that!(actual_data).is_equal_to(&expected_buffer);
    Ok(())
}

#[rstest]
fn modifying_object_doesnt_modify_versions(mut repo: VersionRepo<String>) -> anyhow::Result<()> {
    repo.insert(String::from("Key")).unwrap();
    let version = repo.create_version("Key").unwrap();

    let mut object = repo.object("Key").unwrap();
    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    let object = repo.version_object("Key", version.id()).unwrap();

    assert_that!(object.size()).is_ok_containing(0);

    Ok(())
}

#[rstest]
fn objects_removed_on_rollback(mut repo: VersionRepo<String>) -> anyhow::Result<()> {
    let mut object = repo.insert("test".into()).unwrap();
    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    repo.create_version("test").unwrap();

    repo.rollback()?;

    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.keys().next()).is_none();
    assert_that!(repo.object("test")).is_none();

    Ok(())
}

#[rstest]
fn clear_instance_removes_keys(mut repo: VersionRepo<String>) -> anyhow::Result<()> {
    let mut object = repo.insert("test".into()).unwrap();
    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    repo.clear_instance();

    assert_that!(repo.contains("test")).is_false();
    assert_that!(repo.keys().next()).is_none();
    assert_that!(repo.object("test")).is_none();

    Ok(())
}

#[rstest]
fn rollback_after_clear_instance(mut repo: VersionRepo<String>) -> anyhow::Result<()> {
    let mut object = repo.insert("test".into()).unwrap();
    object.write_all(b"test data")?;
    object.commit()?;
    drop(object);

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert_that!(repo.contains("test")).is_true();
    assert_that!(repo.keys().next()).is_some();
    assert_that!(repo.object("test")).is_some();

    Ok(())
}
