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

use std::collections::HashSet;
use std::io::Read;

use acid_store::repo::content::ContentRepo;
#[cfg(feature = "hash-algorithms")]
use acid_store::repo::content::HashAlgorithm;
use acid_store::repo::{Commit, SwitchInstance, DEFAULT_INSTANCE};
use acid_store::uuid::Uuid;
use common::*;

mod common;

#[rstest]
fn switching_instance_does_not_roll_back(
    mut repo: ContentRepo,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let hash = repo.put(buffer.as_slice())?;

    let repo: ContentRepo = repo.switch_instance(Uuid::new_v4().into())?;
    let repo: ContentRepo = repo.switch_instance(DEFAULT_INSTANCE)?;

    assert_that!(repo.contains(&hash)).is_true();
    assert_that!(repo.object(&hash)).is_some();

    Ok(())
}

#[rstest]
fn switching_instance_does_not_commit(
    mut repo: ContentRepo,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let hash = repo.put(buffer.as_slice())?;

    let repo: ContentRepo = repo.switch_instance(Uuid::new_v4().into())?;
    let mut repo: ContentRepo = repo.switch_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert_that!(repo.contains(&hash)).is_false();
    assert_that!(repo.object(&hash)).is_none();

    Ok(())
}

#[rstest]
fn put_object(mut repo: ContentRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    let hash = repo.put(buffer.as_slice())?;
    assert_that!(repo.contains(hash.as_slice())).is_true();
    Ok(())
}

#[rstest]
fn remove_object(mut repo: ContentRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    let hash = repo.put(buffer.as_slice())?;

    assert_that!(repo.remove(&hash)).is_true();
    assert_that!(repo.contains(&hash)).is_false();
    assert_that!(repo.remove(&hash)).is_false();

    Ok(())
}

#[rstest]
fn get_object(mut repo: ContentRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    let hash = repo.put(buffer.as_slice())?;

    let mut object = repo.object(&hash).unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_that!(actual_data).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
fn list_objects(
    mut repo: ContentRepo,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let hash1 = repo.put(first_buffer.as_slice())?;
    let hash2 = repo.put(second_buffer.as_slice())?;

    assert_that!(repo.list().map(Vec::from).collect::<Vec<_>>())
        .contains_all_of(&[&hash1.to_vec(), &hash2.to_vec()]);

    Ok(())
}

#[cfg(feature = "hash-algorithms")]
#[rstest]
fn change_algorithm(mut repo: ContentRepo) -> anyhow::Result<()> {
    let expected_data = b"Data";
    repo.put(&expected_data[..])?;

    repo.change_algorithm(HashAlgorithm::Blake2b(4))?;
    let expected_hash: &[u8] = &[228, 220, 4, 124];

    assert_that!(repo.contains(expected_hash)).is_true();
    assert_that!(repo.object(expected_hash)).is_some();

    let mut object = repo.object(expected_hash).unwrap();
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;
    drop(object);

    assert_that!(actual_data.as_slice()).is_equal_to(&expected_data[..]);

    Ok(())
}

#[rstest]
fn objects_removed_on_rollback(mut repo: ContentRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    let hash = repo.put(buffer.as_slice())?;

    repo.rollback()?;

    assert_that!(repo.contains(&hash)).is_false();
    assert_that!(repo.object(&hash)).is_none();
    assert_that!(repo.list().next()).is_none();

    Ok(())
}

#[rstest]
fn clear_instance_removes_keys(mut repo: ContentRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    let hash = repo.put(buffer.as_slice())?;

    repo.clear_instance();

    assert_that!(repo.contains(&hash)).is_false();
    assert_that!(repo.list().next()).is_none();
    assert_that!(repo.object(&hash)).is_none();

    Ok(())
}

#[rstest]
fn rollback_after_clear_instance(mut repo: ContentRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    let hash = repo.put(buffer.as_slice())?;

    repo.commit()?;
    repo.clear_instance();
    repo.rollback()?;

    assert_that!(repo.contains(&hash)).is_true();
    assert_that!(repo.list().next()).is_some();
    assert_that!(repo.object(&hash)).is_some();

    Ok(())
}

#[rstest]
fn verify_valid_repository_is_valid(mut repo: ContentRepo, buffer: Vec<u8>) -> anyhow::Result<()> {
    repo.put(buffer.as_slice())?;

    assert_that!(repo.verify()).is_ok_containing(HashSet::new());

    Ok(())
}
