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

use acid_store::repo::{key::KeyRepo, Object, OpenMode, OpenOptions, OpenRepo, RepoConfig};
use acid_store::store::MemoryConfig;
use rand::distributions::Alphanumeric;

const KEY_LEN: usize = 16;

pub struct RepoObject {
    pub repo: KeyRepo<String>,
    pub object: Object,
    pub key: String,
}

impl RepoObject {
    pub fn open(config: RepoConfig) -> anyhow::Result<Self> {
        let mut repo: KeyRepo<String> = open_repo(config)?;
        let rng = SmallRng::from_entropy();
        let key: String = rng.sample_iter(&Alphanumeric).take(KEY_LEN).collect();
        let object = repo.insert(key.clone());
        Ok(RepoObject { repo, object, key })
    }
}

pub fn open_repo<R: OpenRepo>(config: RepoConfig) -> anyhow::Result<R> {
    let store_config = MemoryConfig::new();
    Ok(OpenOptions::new()
        .config(config)
        .mode(OpenMode::CreateNew)
        .open(&store_config)?)
}

#[fixture]
pub fn repo<R: OpenRepo>() -> R {
    open_repo(RepoConfig::default()).unwrap()
}

#[fixture]
pub fn repo_object() -> RepoObject {
    RepoObject::open(RepoConfig::default()).unwrap()
}
