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

use tempfile::tempdir;

use acid_store::repo::content::ContentRepo;
use acid_store::repo::file::FileRepo;
use acid_store::repo::key::KeyRepo;
use acid_store::repo::object::ObjectRepo;
use acid_store::repo::value::ValueRepo;
use acid_store::repo::version::VersionRepo;
use acid_store::repo::{
    Chunking, Compression, ConvertRepo, Encryption, OpenOptions, RepoConfig, ResourceLimit,
};
use acid_store::store::MemoryStore;
use common::directory_store;

mod common;

#[test]
fn set_existing_config_and_create_new_repo() -> anyhow::Result<()> {
    // These are random config values for testing. This should not be used as an example config.
    let mut config = RepoConfig::default();
    config.chunking = Chunking::Fixed { size: 1024 * 16 };
    config.compression = Compression::Deflate { level: 3 };
    config.encryption = Encryption::XChaCha20Poly1305;
    config.memory_limit = ResourceLimit::Moderate;
    config.operations_limit = ResourceLimit::Moderate;

    let repo = OpenOptions::new(MemoryStore::new())
        .config(config.clone())
        .password(b"password")
        .create_new::<ObjectRepo>()?;

    assert_eq!(repo.info().config(), &config);
    Ok(())
}

#[test]
fn configure_and_create_new_repo() -> anyhow::Result<()> {
    // These are random config values for testing. This should not be used as an example config.
    let mut config = RepoConfig::default();
    config.chunking = Chunking::Fixed { size: 1024 * 16 };
    config.compression = Compression::Deflate { level: 3 };
    config.encryption = Encryption::XChaCha20Poly1305;
    config.memory_limit = ResourceLimit::Moderate;
    config.operations_limit = ResourceLimit::Moderate;

    let repo = OpenOptions::new(MemoryStore::new())
        .chunking(Chunking::Fixed { size: 1024 * 16 })
        .compression(Compression::Deflate { level: 3 })
        .encryption(Encryption::XChaCha20Poly1305)
        .memory_limit(ResourceLimit::Moderate)
        .operations_limit(ResourceLimit::Moderate)
        .password(b"password")
        .create_new::<ObjectRepo>()?;

    assert_eq!(repo.info().config(), &config);
    Ok(())
}

#[test]
fn creating_new_existing_repo_errs() -> anyhow::Result<()> {
    let initial_repo: ObjectRepo = OpenOptions::new(MemoryStore::new()).create_new()?;
    let new_repo: Result<ObjectRepo, _> = OpenOptions::new(initial_repo.into_store()).create_new();

    assert!(matches!(new_repo, Err(acid_store::Error::AlreadyExists)));
    Ok(())
}

#[test]
fn opening_or_creating_nonexistent_repo_succeeds() -> anyhow::Result<()> {
    OpenOptions::new(MemoryStore::new()).create::<ObjectRepo>()?;
    Ok(())
}

#[test]
fn opening_or_creating_existing_repo_succeeds() -> anyhow::Result<()> {
    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<ObjectRepo>()?;
    OpenOptions::new(initial_repo.into_store()).create::<ObjectRepo>()?;
    Ok(())
}

#[test]
fn opening_nonexistent_repo_errs() {
    let repo: Result<ObjectRepo, _> = OpenOptions::new(MemoryStore::new()).open();
    assert!(matches!(repo, Err(acid_store::Error::NotFound)));
}

#[test]
fn opening_with_invalid_password_errs() -> anyhow::Result<()> {
    let repo: ObjectRepo = OpenOptions::new(MemoryStore::new())
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .create_new()?;
    let new_repo: Result<ObjectRepo, _> = OpenOptions::new(repo.into_store())
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Not the password")
        .open();

    assert!(matches!(new_repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn creating_without_password_errs() -> anyhow::Result<()> {
    let repo = OpenOptions::new(MemoryStore::new())
        .encryption(Encryption::XChaCha20Poly1305)
        .create_new::<ObjectRepo>();
    assert!(matches!(repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn opening_without_password_errs() -> anyhow::Result<()> {
    let repo: ObjectRepo = OpenOptions::new(MemoryStore::new())
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .create_new()?;
    let new_repo: Result<ObjectRepo, _> = OpenOptions::new(repo.into_store()).open();
    assert!(matches!(new_repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn opening_locked_repo_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;

    let store = directory_store(temp_dir.as_ref())?;
    let store_copy = directory_store(temp_dir.as_ref())?;

    let mut repo: ObjectRepo = OpenOptions::new(store).create_new()?;
    repo.commit()?;

    let new_repo: Result<ObjectRepo, _> = OpenOptions::new(store_copy).open();

    assert!(matches!(new_repo, Err(acid_store::Error::Locked)));
    Ok(())
}

#[test]
fn opening_existing_repo_of_different_type_errs() -> anyhow::Result<()> {
    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
    let new_repo = OpenOptions::new(initial_repo.into_repo()?.into_store()).open::<ContentRepo>();
    assert!(matches!(
        new_repo,
        Err(acid_store::Error::UnsupportedFormat)
    ));

    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
    let new_repo =
        OpenOptions::new(initial_repo.into_repo()?.into_store()).open::<VersionRepo<String>>();
    assert!(matches!(
        new_repo,
        Err(acid_store::Error::UnsupportedFormat)
    ));

    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
    let new_repo = OpenOptions::new(initial_repo.into_repo()?.into_store()).open::<FileRepo>();
    assert!(matches!(
        new_repo,
        Err(acid_store::Error::UnsupportedFormat)
    ));

    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
    let new_repo =
        OpenOptions::new(initial_repo.into_repo()?.into_store()).open::<ValueRepo<String>>();
    assert!(matches!(
        new_repo,
        Err(acid_store::Error::UnsupportedFormat)
    ));

    Ok(())
}

#[test]
fn opening_or_creating_existing_repo_of_different_type_errs() -> anyhow::Result<()> {
    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
    let new_repo = OpenOptions::new(initial_repo.into_repo()?.into_store()).create::<ContentRepo>();
    assert!(matches!(
        new_repo,
        Err(acid_store::Error::UnsupportedFormat)
    ));

    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
    let new_repo =
        OpenOptions::new(initial_repo.into_repo()?.into_store()).create::<VersionRepo<String>>();
    assert!(matches!(
        new_repo,
        Err(acid_store::Error::UnsupportedFormat)
    ));

    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
    let new_repo = OpenOptions::new(initial_repo.into_repo()?.into_store()).create::<FileRepo>();
    assert!(matches!(
        new_repo,
        Err(acid_store::Error::UnsupportedFormat)
    ));

    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<KeyRepo<String>>()?;
    let new_repo =
        OpenOptions::new(initial_repo.into_repo()?.into_store()).create::<ValueRepo<String>>();
    assert!(matches!(
        new_repo,
        Err(acid_store::Error::UnsupportedFormat)
    ));

    Ok(())
}

#[test]
fn open_or_create_existing_repo() -> anyhow::Result<()> {
    let initial_repo = OpenOptions::new(MemoryStore::new()).create_new::<ObjectRepo>()?;
    let store = initial_repo.into_store();
    OpenOptions::new(store).create::<ObjectRepo>()?;
    Ok(())
}

#[test]
fn open_or_create_nonexistent_repo() -> anyhow::Result<()> {
    OpenOptions::new(MemoryStore::new()).create::<ObjectRepo>()?;
    Ok(())
}
