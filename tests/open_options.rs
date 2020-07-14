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

use acid_store::repo::object::ObjectRepo;
use acid_store::repo::{Encryption, OpenOptions};
use acid_store::store::{DirectoryStore, MemoryStore, OpenOption, OpenStore};

mod common;

#[test]
fn creating_existing_repo_errs() -> anyhow::Result<()> {
    let initial_repo: ObjectRepo<_> = OpenOptions::new(MemoryStore::new()).create_new()?;
    let new_repo: Result<ObjectRepo<_>, _> =
        OpenOptions::new(initial_repo.into_store()).create_new();

    assert!(matches!(new_repo, Err(acid_store::Error::AlreadyExists)));
    Ok(())
}

#[test]
fn opening_nonexistent_repo_errs() {
    let repo: Result<ObjectRepo<_>, _> = OpenOptions::new(MemoryStore::new()).open();
    assert!(matches!(repo, Err(acid_store::Error::NotFound)));
}

#[test]
fn opening_with_invalid_password_errs() -> anyhow::Result<()> {
    let repo: ObjectRepo<_> = OpenOptions::new(MemoryStore::new())
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .create_new()?;
    let new_repo: Result<ObjectRepo<_>, _> = OpenOptions::new(repo.into_store())
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
        .create_new::<ObjectRepo<_>>();
    assert!(matches!(repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn opening_without_password_errs() -> anyhow::Result<()> {
    let repo: ObjectRepo<_> = OpenOptions::new(MemoryStore::new())
        .encryption(Encryption::XChaCha20Poly1305)
        .password(b"Password")
        .create_new()?;
    let new_repo: Result<ObjectRepo<_>, _> = OpenOptions::new(repo.into_store()).open();
    assert!(matches!(new_repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn creating_with_unnecessary_password_errs() -> anyhow::Result<()> {
    let repo: Result<ObjectRepo<_>, _> = OpenOptions::new(MemoryStore::new())
        .password(b"Unnecessary password")
        .create_new();
    assert!(matches!(repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn opening_with_unnecessary_password_errs() -> anyhow::Result<()> {
    let repo: ObjectRepo<_> = OpenOptions::new(MemoryStore::new()).create_new()?;
    let new_repo: Result<ObjectRepo<_>, _> = OpenOptions::new(repo.into_store())
        .password(b"Unnecessary password")
        .open();
    assert!(matches!(new_repo, Err(acid_store::Error::Password)));
    Ok(())
}

#[test]
fn opening_locked_repo_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;

    let store = DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::CREATE_NEW)?;
    let store_copy = DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::empty())?;

    let mut repo: ObjectRepo<_> = OpenOptions::new(store).create_new()?;
    repo.commit()?;

    let new_repo: Result<ObjectRepo<_>, _> = OpenOptions::new(store_copy).open();

    assert!(matches!(new_repo, Err(acid_store::Error::Locked)));
    Ok(())
}
