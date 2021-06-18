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

use acid_store::repo::state::StateRepo;
use acid_store::repo::{Commit, OpenMode, OpenOptions, RestoreSavepoint};
use acid_store::store::MemoryConfig;

fn create_repo(config: &MemoryConfig) -> acid_store::Result<StateRepo<String>> {
    OpenOptions::new().mode(OpenMode::CreateNew).open(config)
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repository = create_repo(&config)?;
    repository.commit()?;
    drop(repository);
    OpenOptions::new().open::<StateRepo<String>, _>(&config)?;
    Ok(())
}

#[test]
fn state_is_persisted_on_commit() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    *repo.state_mut() = String::from("New state");
    repo.commit()?;
    drop(repo);
    let repo: StateRepo<String> = OpenOptions::new().open(&config)?;

    assert_eq!(repo.state(), "New state");

    Ok(())
}

#[test]
fn state_is_rolled_back() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    *repo.state_mut() = String::from("Initial state");
    repo.commit()?;
    *repo.state_mut() = String::from("New state");
    repo.rollback()?;

    assert_eq!(repo.state(), "Initial state");

    Ok(())
}

#[test]
fn state_is_restored_by_savepoint() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    *repo.state_mut() = String::from("Initial state");
    let savepoint = repo.savepoint()?;
    *repo.state_mut() = String::from("New state");
    repo.restore(&savepoint)?;

    assert_eq!(repo.state(), "Initial state");

    Ok(())
}

#[test]
fn state_is_defaulted_on_clear_instance() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let mut repo = create_repo(&config)?;

    *repo.state_mut() = String::from("Initial state");
    repo.commit()?;

    repo.clear_instance();

    assert_eq!(repo.state(), &String::default());

    Ok(())
}
