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

use rand::prelude::*;
use rstest::*;

use acid_store::repo::{
    key::KeyRepo, InstanceId, Object, OpenMode, OpenOptions, OpenRepo, RepoConfig, DEFAULT_INSTANCE,
};
use acid_store::store::MemoryConfig;
use rand::distributions::Alphanumeric;

const KEY_LEN: usize = 16;

const PASSWORD_LEN: usize = 16;

/// A test helper which encapsulates a repository and an object.
pub struct RepoObject {
    pub repo: KeyRepo<String>,
    pub object: Object,
    pub key: String,
}

impl RepoObject {
    pub fn new(config: RepoConfig) -> anyhow::Result<Self> {
        let mut repo: KeyRepo<String> = create_repo(config)?;
        let rng = SmallRng::from_entropy();
        let key: String = rng.sample_iter(&Alphanumeric).take(KEY_LEN).collect();
        let object = repo.insert(key.clone());
        Ok(RepoObject { repo, object, key })
    }
}

/// A test helper for opening multiple repositories backed by the same data store.
pub struct RepoStore {
    pub store: MemoryConfig,
    pub config: RepoConfig,
    pub password: String,
    pub instance: InstanceId,
}

impl RepoStore {
    pub fn new(config: RepoConfig) -> Self {
        let rng = SmallRng::from_entropy();
        let password: String = rng.sample_iter(&Alphanumeric).take(PASSWORD_LEN).collect();
        RepoStore {
            store: MemoryConfig::new(),
            config,
            password,
            instance: DEFAULT_INSTANCE,
        }
    }

    /// Create a new repository.
    pub fn create<R: OpenRepo>(&self) -> acid_store::Result<R> {
        OpenOptions::new()
            .config(self.config.clone())
            .password(self.password.as_bytes())
            .instance(self.instance)
            .mode(OpenMode::CreateNew)
            .open(&self.store)
    }

    /// Open an existing repository.
    pub fn open<R: OpenRepo>(&self) -> acid_store::Result<R> {
        OpenOptions::new()
            .config(self.config.clone())
            .password(self.password.as_bytes())
            .instance(self.instance)
            .open(&self.store)
    }
}

pub fn create_repo<R: OpenRepo>(config: RepoConfig) -> anyhow::Result<R> {
    let store_config = MemoryConfig::new();
    Ok(OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?)
}

/// A test fixture which provides a new empty repository.
#[fixture]
pub fn repo<R: OpenRepo>() -> R {
    create_repo(RepoConfig::default()).unwrap()
}

/// A test fixture which provides a new empty `RepoObject`.
#[fixture]
pub fn repo_object() -> RepoObject {
    RepoObject::new(RepoConfig::default()).unwrap()
}

/// A test fixture which provides a new empty `RepoStore`.
#[fixture]
pub fn repo_store() -> RepoStore {
    RepoStore::new(RepoConfig::default())
}
