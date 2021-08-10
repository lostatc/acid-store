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
    feature = "repo-value",
    feature = "repo-version",
    feature = "encryption",
    feature = "compression"
))]

use acid_store::repo::key::KeyRepo;
use acid_store::repo::value::ValueRepo;
use acid_store::repo::version::VersionRepo;
use acid_store::repo::{
    Chunking, Commit, Compression, Encryption, OpenMode, OpenOptions, RepoConfig, ResourceLimit,
};
use acid_store::store::MemoryConfig;
use common::*;

mod common;

#[rstest]
fn set_existing_config_and_create_new_repo(mut repo_store: RepoStore) -> anyhow::Result<()> {
    // These are random config values for testing. This should not be used as an example config.
    repo_store.config.chunking = Chunking::Fixed { size: 1024 * 16 };
    repo_store.config.compression = Compression::Lz4 { level: 2 };
    repo_store.config.encryption = Encryption::XChaCha20Poly1305;
    repo_store.config.memory_limit = ResourceLimit::Moderate;
    repo_store.config.operations_limit = ResourceLimit::Moderate;

    let repo: KeyRepo<String> = repo_store.create()?;

    assert_that!(repo.info().config()).is_equal_to(&repo_store.config);

    Ok(())
}

#[rstest]
fn configure_and_create_new_repo() -> anyhow::Result<()> {
    // These are random config values for testing. This should not be used as an example config.
    let mut expected_config = RepoConfig::default();
    expected_config.chunking = Chunking::Fixed { size: 1024 * 16 };
    expected_config.compression = Compression::Lz4 { level: 2 };
    expected_config.encryption = Encryption::XChaCha20Poly1305;
    expected_config.memory_limit = ResourceLimit::Moderate;
    expected_config.operations_limit = ResourceLimit::Moderate;

    let config = MemoryConfig::new();
    let repo: KeyRepo<String> = OpenOptions::new()
        .chunking(Chunking::Fixed { size: 1024 * 16 })
        .compression(Compression::Lz4 { level: 2 })
        .encryption(Encryption::XChaCha20Poly1305)
        .memory_limit(ResourceLimit::Moderate)
        .operations_limit(ResourceLimit::Moderate)
        .password(b"password")
        .mode(OpenMode::CreateNew)
        .open(&config)?;

    assert_that!(repo.info().config()).is_equal_to(&expected_config);

    Ok(())
}

#[rstest]
fn creating_new_existing_repo_errs(repo_store: RepoStore) -> anyhow::Result<()> {
    repo_store.create::<KeyRepo<String>>()?;

    assert_that!(OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&repo_store.store))
    .is_err_variant(acid_store::Error::AlreadyExists);

    Ok(())
}

#[rstest]
fn opening_or_creating_nonexistent_repo_succeeds() {
    let config = MemoryConfig::new();
    assert_that!(OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<KeyRepo<String>, _>(&config))
    .is_ok();
}

#[rstest]
fn opening_or_creating_existing_repo_succeeds(repo_store: RepoStore) -> anyhow::Result<()> {
    repo_store.create::<KeyRepo<String>>()?;
    assert_that!(OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<KeyRepo<String>, _>(&repo_store.store))
    .is_ok();
    Ok(())
}

#[rstest]
fn opening_nonexistent_repo_errs(repo_store: RepoStore) {
    assert_that!(repo_store.open::<KeyRepo<String>>()).is_err_variant(acid_store::Error::NotFound);
}

#[rstest]
fn opening_with_invalid_password_errs(mut repo_store: RepoStore) -> anyhow::Result<()> {
    repo_store.config.encryption = Encryption::XChaCha20Poly1305;
    repo_store.create::<KeyRepo<String>>()?;
    repo_store.password = String::from("Not the password");

    assert_that!(repo_store.open::<KeyRepo<String>>()).is_err_variant(acid_store::Error::Password);

    Ok(())
}

#[rstest]
fn creating_without_password_errs() {
    let config = MemoryConfig::new();
    assert_that!(OpenOptions::new()
        .encryption(Encryption::XChaCha20Poly1305)
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config))
    .is_err_variant(acid_store::Error::Password);
}

#[rstest]
fn opening_without_password_errs(mut repo_store: RepoStore) -> anyhow::Result<()> {
    repo_store.config.encryption = Encryption::XChaCha20Poly1305;
    repo_store.create::<KeyRepo<String>>()?;
    assert_that!(OpenOptions::new().open::<KeyRepo<String>, _>(&repo_store.store))
        .is_err_variant(acid_store::Error::Password);
    Ok(())
}

#[rstest]
fn open_or_create_existing_repo(repo_store: RepoStore) -> anyhow::Result<()> {
    repo_store.create::<KeyRepo<String>>()?;
    assert_that!(OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<KeyRepo<String>, _>(&repo_store.store))
    .is_ok();
    Ok(())
}

#[rstest]
fn open_or_create_nonexistent_repo() {
    let config = MemoryConfig::new();
    assert_that!(OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<KeyRepo<String>, _>(&config))
    .is_ok();
}

#[rstest]
fn opening_existing_repo_of_different_type_errs(repo_store: RepoStore) -> anyhow::Result<()> {
    let mut repo = repo_store.create::<VersionRepo<String>>()?;
    repo.commit()?;
    drop(repo);
    assert_that!(repo_store.open::<ValueRepo<String>>())
        .is_err_variant(acid_store::Error::UnsupportedRepo);
    Ok(())
}

#[rstest]
fn existing_locks_are_respected(repo_store: RepoStore) -> anyhow::Result<()> {
    let _repo: KeyRepo<String> = repo_store.create()?;
    assert_that!(repo_store.open::<KeyRepo<String>>()).is_err_variant(acid_store::Error::Locked);
    Ok(())
}

#[rstest]
fn existing_locks_are_removed(mut repo_store: RepoStore) -> anyhow::Result<()> {
    let _repo: KeyRepo<String> = repo_store.create()?;
    repo_store.handler = Box::new(|_| true);
    assert_that!(repo_store.open::<KeyRepo<String>>()).is_ok();
    Ok(())
}

#[rstest]
fn lock_handler_is_passed_context_of_existing_lock(
    mut repo_store: RepoStore,
) -> anyhow::Result<()> {
    repo_store.context = b"context value".to_vec();
    let _repo: KeyRepo<String> = repo_store.create()?;
    repo_store.handler = Box::new(|context| {
        assert_that!(context).is_equal_to(&b"context value"[..]);
        true
    });
    assert_that!(repo_store.open::<KeyRepo<String>>()).is_ok();
    Ok(())
}
