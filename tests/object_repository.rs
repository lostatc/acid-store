/*
 * Copyright 2019 Wren Powell
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

use uuid::Uuid;

use common::{create_repo, ARCHIVE_CONFIG, PASSWORD};
use data_store::repo::{LockStrategy, ObjectRepository};
use data_store::store::MemoryStore;

#[macro_use]
mod common;

#[test]
fn creating_existing_repo_errs() -> anyhow::Result<()> {
    let initial_repo = create_repo()?;
    let new_repo = ObjectRepository::<String, _>::create_repo(
        initial_repo.into_store(),
        ARCHIVE_CONFIG,
        Some(PASSWORD),
    );

    assert_match!(new_repo.unwrap_err(), data_store::Error::AlreadyExists);
    Ok(())
}

#[test]
fn opening_nonexistent_repo_errs() {
    let repository = ObjectRepository::<String, _>::open_repo(
        MemoryStore::open(),
        Some(PASSWORD),
        LockStrategy::Abort,
    );

    assert_match!(repository.unwrap_err(), data_store::Error::NotFound);
}

#[test]
fn opening_with_invalid_password_errs() -> anyhow::Result<()> {
    let repository = create_repo()?;
    let repository = ObjectRepository::<String, _>::open_repo(
        repository.into_store(),
        Some(b"not the password"),
        LockStrategy::Abort,
    );

    assert_match!(repository.unwrap_err(), data_store::Error::Password);
    Ok(())
}

#[test]
fn opening_with_wrong_key_type_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.insert("Test".into());
    repository.commit()?;

    let repository = ObjectRepository::<isize, _>::open_repo(
        repository.into_store(),
        Some(PASSWORD),
        LockStrategy::Abort,
    );

    assert_match!(repository.unwrap_err(), data_store::Error::KeyType);
    Ok(())
}
