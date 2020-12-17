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

use acid_store::repo::content::ContentRepo;
use acid_store::repo::file::FileRepo;
use acid_store::repo::key::KeyRepo;
use acid_store::repo::object::ObjectRepo;
use acid_store::repo::value::ValueRepo;
use acid_store::repo::version::VersionRepo;
use acid_store::repo::{
    Chunking, Compression, Encryption, OpenMode, OpenOptions, RepoConfig, ResourceLimit,
};
use acid_store::store::MemoryConfig;

mod common;

#[test]
fn set_existing_config_and_create_new_repo() -> anyhow::Result<()> {
    // These are random config values for testing. This should not be used as an example config.
    let mut repo_config = RepoConfig::default();
    repo_config.chunking = Chunking::Fixed { size: 1024 * 16 };
    repo_config.compression = Compression::Lz4 { level: 2 };
    repo_config.encryption = Encryption::XChaCha20Poly1305;
    repo_config.memory_limit = ResourceLimit::Moderate;
    repo_config.operations_limit = ResourceLimit::Moderate;

    let config = MemoryConfig::new();
    let repo: ObjectRepo = OpenOptions::new()
        .config(repo_config.clone())
        .password(b"password")
        .mode(OpenMode::CreateNew)
        .open(&config)?;

    assert_eq!(repo.info().config(), &repo_config);
    Ok(())
}

#[test]
fn configure_and_create_new_repo() -> anyhow::Result<()> {
    // These are random config values for testing. This should not be used as an example config.
    let mut repo_config = RepoConfig::default();
    repo_config.chunking = Chunking::Fixed { size: 1024 * 16 };
    repo_config.compression = Compression::Lz4 { level: 2 };
    repo_config.encryption = Encryption::XChaCha20Poly1305;
    repo_config.memory_limit = ResourceLimit::Moderate;
    repo_config.operations_limit = ResourceLimit::Moderate;

    let config = MemoryConfig::new();
    let repo: ObjectRepo = OpenOptions::new()
        .chunking(Chunking::Fixed { size: 1024 * 16 })
        .compression(Compression::Lz4 { level: 2 })
        .encryption(Encryption::XChaCha20Poly1305)
        .memory_limit(ResourceLimit::Moderate)
        .operations_limit(ResourceLimit::Moderate)
        .password(b"password")
        .mode(OpenMode::CreateNew)
        .open(&config)?;

    assert_eq!(repo.info().config(), &repo_config);
    Ok(())
}

#[test]
fn creating_new_existing_repo_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<ObjectRepo, _>(&config)?;
    let new_repo: Result<ObjectRepo, _> =
        OpenOptions::new().mode(OpenMode::CreateNew).open(&config);

    assert!(matches!(new_repo, Err(acid_store::Error::AlreadyExists)));
    Ok(())
}

#[test]
fn opening_or_creating_nonexistent_repo_succeeds() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<ObjectRepo, _>(&config)?;
    Ok(())
}

#[test]
fn opening_or_creating_existing_repo_succeeds() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<ObjectRepo, _>(&config)?;
    OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<ObjectRepo, _>(&config)?;
    Ok(())
}

#[test]
fn opening_nonexistent_repo_errs() {
    let config = MemoryConfig::new();
    let repo: Result<ObjectRepo, _> = OpenOptions::new().open(&config);
    assert!(matches!(repo, Err(acid_store::Error::NotFound)));
}

#[test]
fn opening_with_invalid_password_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open::<ObjectRepo, _>(&config)?;
    let new_repo: Result<ObjectRepo, _> = OpenOptions::new()
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Not the password")
        .open(&config);

    assert!(matches!(new_repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn creating_without_password_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    let repo = OpenOptions::new()
        .encryption(Encryption::XChaCha20Poly1305)
        .mode(OpenMode::CreateNew)
        .open::<ObjectRepo, _>(&config);
    assert!(matches!(repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn opening_without_password_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open::<ObjectRepo, _>(&config)?;
    let new_repo: Result<ObjectRepo, _> = OpenOptions::new().open(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn opening_locked_repo_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();

    let mut repo: ObjectRepo = OpenOptions::new().mode(OpenMode::CreateNew).open(&config)?;
    repo.commit()?;

    let new_repo: Result<ObjectRepo, _> = OpenOptions::new().open(&config);

    assert!(matches!(new_repo, Err(acid_store::Error::Locked)));
    Ok(())
}

#[test]
fn opening_existing_repo_of_different_type_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config)?;
    let new_repo = OpenOptions::new().open::<ContentRepo, _>(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::UnsupportedRepo)));

    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config)?;
    let new_repo = OpenOptions::new().open::<VersionRepo<String>, _>(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::UnsupportedRepo)));

    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config)?;
    let new_repo = OpenOptions::new().open::<FileRepo, _>(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::UnsupportedRepo)));

    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config)?;
    let new_repo = OpenOptions::new().open::<ValueRepo<String>, _>(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::UnsupportedRepo)));

    Ok(())
}

#[test]
fn opening_or_creating_existing_repo_of_different_type_errs() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config)?;
    let new_repo = OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<ContentRepo, _>(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::UnsupportedRepo)));

    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config)?;
    let new_repo = OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<VersionRepo<String>, _>(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::UnsupportedRepo)));

    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config)?;
    let new_repo = OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<FileRepo, _>(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::UnsupportedRepo)));

    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<KeyRepo<String>, _>(&config)?;
    let new_repo = OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<ValueRepo<String>, _>(&config);
    assert!(matches!(new_repo, Err(acid_store::Error::UnsupportedRepo)));

    Ok(())
}

#[test]
fn open_or_create_existing_repo() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open::<ObjectRepo, _>(&config)?;
    OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<ObjectRepo, _>(&config)?;
    Ok(())
}

#[test]
fn open_or_create_nonexistent_repo() -> anyhow::Result<()> {
    let config = MemoryConfig::new();
    OpenOptions::new()
        .mode(OpenMode::Create)
        .open::<ObjectRepo, _>(&config)?;
    Ok(())
}
