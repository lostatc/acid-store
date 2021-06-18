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

use std::io::Write;

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{
    Commit, OpenMode, OpenOptions, RepoConfig, SwitchInstance, DEFAULT_INSTANCE,
};
use acid_store::store::MemoryConfig;
use acid_store::uuid::Uuid;
use test_case::test_case;

use common::random_buffer;

mod common;

fn create_repo(
    repo_config: RepoConfig,
    store_config: &MemoryConfig,
) -> acid_store::Result<KeyRepo<String>> {
    OpenOptions::new()
        .config(repo_config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(store_config)
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn switching_instance_does_not_roll_back(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let repo: KeyRepo<String> = repo.switch_instance(Uuid::new_v4())?;
    let repo: KeyRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;

    assert!(repo.contains("test"));
    assert!(repo.object("test").is_some());

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn switching_instance_does_not_commit(repo_config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo = create_repo(repo_config, &store_config)?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    drop(object);

    let repo: KeyRepo<String> = repo.switch_instance(Uuid::new_v4())?;
    let mut repo: KeyRepo<String> = repo.switch_instance(DEFAULT_INSTANCE)?;
    repo.rollback()?;

    assert!(!repo.contains("test"));
    assert!(repo.object("test").is_none());

    Ok(())
}
