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

use uuid::Uuid;

use acid_store::repo::state::StateRepo;
use acid_store::repo::{Commit, RestoreSavepoint, SwitchInstance};
use common::*;

mod common;

#[rstest]
fn state_is_persisted_on_commit(repo_store: RepoStore) -> anyhow::Result<()> {
    let mut repo: StateRepo<String> = repo_store.create()?;
    *repo.state_mut() = String::from("New state");
    repo.commit()?;
    drop(repo);
    let repo: StateRepo<String> = repo_store.open()?;

    assert_that!(repo.state()).is_equal_to(&String::from("New state"));

    Ok(())
}

#[rstest]
fn state_is_rolled_back(mut repo: StateRepo<String>) -> anyhow::Result<()> {
    *repo.state_mut() = String::from("Initial state");
    repo.commit()?;
    *repo.state_mut() = String::from("New state");
    repo.rollback()?;

    assert_that!(repo.state()).is_equal_to(&String::from("Initial state"));

    Ok(())
}

#[rstest]
fn state_is_restored_by_savepoint(mut repo: StateRepo<String>) -> anyhow::Result<()> {
    *repo.state_mut() = String::from("Initial state");
    let savepoint = repo.savepoint()?;
    *repo.state_mut() = String::from("New state");
    repo.restore(&savepoint)?;

    assert_that!(repo.state()).is_equal_to(&String::from("Initial state"));

    Ok(())
}

#[rstest]
fn state_is_defaulted_on_clear_instance(mut repo: StateRepo<String>) -> anyhow::Result<()> {
    *repo.state_mut() = String::from("Initial state");
    repo.commit()?;

    repo.clear_instance();

    assert_that!(repo.state()).is_equal_to(&String::default());

    Ok(())
}

#[rstest]
fn ids_from_different_instances_are_not_valid(mut repo: StateRepo<String>) -> anyhow::Result<()> {
    let id = repo.create();

    let mut repo: StateRepo<String> = repo.switch_instance(Uuid::new_v4().into())?;

    assert_that!(repo.contains(id)).is_false();
    assert_that!(repo.object(id)).is_none();
    assert_that!(repo.copy(id)).is_none();
    assert_that!(repo.remove(id)).is_false();

    Ok(())
}
