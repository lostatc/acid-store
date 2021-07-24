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

#![macro_use]

use rstest_reuse::{self, *};

use super::repository::RepoObject;
use crate::common::RepoStore;
use acid_store::repo::{key::KeyRepo, Chunking, Compression, Encryption, Packing, RepoConfig};

/// The repository config used for testing fixed-size chunking.
pub fn fixed_config() -> RepoConfig {
    let mut config = RepoConfig::default();
    config.chunking = Chunking::Fixed { size: 256 };
    config.packing = Packing::None;
    config.encryption = Encryption::None;
    config.compression = Compression::None;
    config
}

/// The repository config used for testing encryption and compression.
pub fn encoding_config() -> RepoConfig {
    let mut config = fixed_config();
    config.encryption = Encryption::XChaCha20Poly1305;
    config.compression = Compression::Lz4 { level: 1 };
    config
}

/// The repository config used for testing ZPAQ chunking.
pub fn zpaq_config() -> RepoConfig {
    let mut config = fixed_config();
    config.chunking = Chunking::Zpaq { bits: 8 };
    config
}

/// The repository config used for testing packing with a size smaller than the chunk size.
pub fn fixed_packing_small_config() -> RepoConfig {
    let mut config = fixed_config();
    // Smaller than the chunk size and not a factor of it.
    config.packing = Packing::Fixed(100);
    config
}

/// The repository config used for testing packing with a size larger than the chunk size.
pub fn fixed_packing_large_config() -> RepoConfig {
    let mut config = fixed_config();
    // Larger than the chunk size and not a multiple of it.
    config.packing = Packing::Fixed(300);
    config
}

/// The repository config used for testing packing with ZPAQ chunking.
pub fn zpaq_packing_config() -> RepoConfig {
    let mut config = fixed_config();
    config.packing = Packing::Fixed(256);
    config
}

/// A parameterized test template which provides several different repository configurations.
#[template]
#[rstest]
#[case(fixed_config())]
#[case(encoding_config())]
#[case(zpaq_config())]
#[case(fixed_packing_small_config())]
#[case(fixed_packing_large_config())]
#[case(zpaq_packing_config())]
pub fn config(#[case] config: RepoConfig) {}

/// A parameterized test template which provides several differently-configured repositories.
#[template]
#[rstest]
#[case(create_repo(fixed_config()).unwrap())]
#[case(create_repo(encoding_config()).unwrap())]
#[case(create_repo(zpaq_config()).unwrap())]
#[case(create_repo(fixed_packing_small_config()).unwrap())]
#[case(create_repo(fixed_packing_large_config()).unwrap())]
#[case(create_repo(zpaq_packing_config()).unwrap())]
pub fn repo_config(#[case] repo: KeyRepo<String>) {}

/// A parameterized test template which provides several differently-configured `RepoObject` values.
#[template]
#[rstest]
#[case(RepoObject::new(fixed_config()).unwrap())]
#[case(RepoObject::new(encoding_config()).unwrap())]
#[case(RepoObject::new(zpaq_config()).unwrap())]
#[case(RepoObject::new(fixed_packing_small_config()).unwrap())]
#[case(RepoObject::new(fixed_packing_large_config()).unwrap())]
#[case(RepoObject::new(zpaq_packing_config()).unwrap())]
pub fn object_config(#[case] repo_object: RepoObject) {}

/// A parameterized test template which provides several differently-configured `RepoStore` values.
#[template]
#[rstest]
#[case(RepoStore::new(fixed_config()))]
#[case(RepoStore::new(encoding_config()))]
#[case(RepoStore::new(zpaq_config()))]
#[case(RepoStore::new(fixed_packing_small_config()))]
#[case(RepoStore::new(fixed_packing_large_config()))]
#[case(RepoStore::new(zpaq_packing_config()))]
pub fn store_config(#[case] repo_store: RepoStore) {}
