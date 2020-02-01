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

#![cfg(feature = "repo-file")]

use acid_store::repo::{FileMetadata, FileRepository};
use acid_store::store::MemoryStore;
use common::{ARCHIVE_CONFIG, PASSWORD};

#[macro_use]
mod common;

fn create_repo() -> acid_store::Result<FileRepository<MemoryStore>> {
    FileRepository::create_repo(MemoryStore::new(), ARCHIVE_CONFIG, Some(PASSWORD))
}

#[test]
fn creating_existing_file_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.create("home", &FileMetadata::directory())?;
    let result = repository.create("home", &FileMetadata::directory());

    assert_match!(result.unwrap_err(), acid_store::Error::AlreadyExists);
    Ok(())
}

#[test]
fn creating_file_without_parent_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    // Creating a directory without a parent fails.
    let result = repository.create("home/lostatc", &FileMetadata::directory());
    assert_match!(result.unwrap_err(), acid_store::Error::InvalidPath);

    // Creating a directory as a child of a file fails.
    repository.create("home", &FileMetadata::file())?;
    let result = repository.create("home/lostatc", &FileMetadata::directory());
    assert_match!(result.unwrap_err(), acid_store::Error::InvalidPath);

    Ok(())
}

#[test]
fn create_parents() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.create_parents("home/lostatc/test", &FileMetadata::file())?;

    assert!(repository.metadata("home/lostatc/test")?.is_file());
    assert!(repository.metadata("home/lostatc")?.is_directory());
    assert!(repository.metadata("home")?.is_directory());

    Ok(())
}
