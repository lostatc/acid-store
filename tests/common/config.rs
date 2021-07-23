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

use once_cell::sync::Lazy;
use rstest_reuse::{self, *};

use super::repository::RepoObject;
use acid_store::repo::{
    key::KeyRepo, Chunking, Compression, Encryption, OpenMode, OpenOptions, Packing, RepoConfig,
};

/// The repository config used for testing fixed-size chunking.
pub static FIXED_CONFIG: Lazy<RepoConfig> = Lazy::new(|| {
    let mut config = RepoConfig::default();
    config.chunking = Chunking::Fixed { size: 256 };
    config.packing = Packing::None;
    config.encryption = Encryption::None;
    config.compression = Compression::None;
    config
});

/// The repository config used for testing encryption and compression.
pub static ENCODING_CONFIG: Lazy<RepoConfig> = Lazy::new(|| {
    let mut config = FIXED_CONFIG.to_owned();
    config.encryption = Encryption::XChaCha20Poly1305;
    config.compression = Compression::Lz4 { level: 1 };
    config
});

/// The repository config used for testing ZPAQ chunking.
pub static ZPAQ_CONFIG: Lazy<RepoConfig> = Lazy::new(|| {
    let mut config = FIXED_CONFIG.to_owned();
    config.chunking = Chunking::Zpaq { bits: 8 };
    config
});

/// The repository config used for testing packing with a size smaller than the chunk size.
pub static FIXED_PACKING_SMALL_CONFIG: Lazy<RepoConfig> = Lazy::new(|| {
    let mut config = FIXED_CONFIG.to_owned();
    // Smaller than the chunk size and not a factor of it.
    config.packing = Packing::Fixed(100);
    config
});

/// The repository config used for testing packing with a size larger than the chunk size.
pub static FIXED_PACKING_LARGE_CONFIG: Lazy<RepoConfig> = Lazy::new(|| {
    let mut config = FIXED_CONFIG.to_owned();
    // Larger than the chunk size and not a multiple of it.
    config.packing = Packing::Fixed(300);
    config
});

/// The repository config used for testing packing with ZPAQ chunking.
pub static ZPAQ_PACKING_CONFIG: Lazy<RepoConfig> = Lazy::new(|| {
    let mut config = ZPAQ_CONFIG.to_owned();
    config.packing = Packing::Fixed(256);
    config
});

#[template]
#[rstest]
#[case(*FIXED_CONFIG)]
#[case(*ENCODING_CONFIG)]
#[case(*ZPAQ_CONFIG)]
#[case(*FIXED_PACKING_SMALL_CONFIG)]
#[case(*FIXED_PACKING_LARGE_CONFIG)]
#[case(*ZPAQ_PACKING_CONFIG)]
pub fn config(#[case] config: RepoConfig) {}

#[template]
#[rstest]
#[case(open_repo(*FIXED_CONFIG).unwrap())]
#[case(open_repo(*ENCODING_CONFIG).unwrap())]
#[case(open_repo(*ZPAQ_CONFIG).unwrap())]
#[case(open_repo(*FIXED_PACKING_SMALL_CONFIG).unwrap())]
#[case(open_repo(*FIXED_PACKING_LARGE_CONFIG).unwrap())]
#[case(open_repo(*ZPAQ_PACKING_CONFIG).unwrap())]
pub fn repo_config(#[case] repo: KeyRepo<String>) {}

#[template]
#[rstest]
#[case(RepoObject::open(*FIXED_CONFIG).unwrap())]
#[case(RepoObject::open(*ENCODING_CONFIG).unwrap())]
#[case(RepoObject::open(*ZPAQ_CONFIG).unwrap())]
#[case(RepoObject::open(*FIXED_PACKING_SMALL_CONFIG).unwrap())]
#[case(RepoObject::open(*FIXED_PACKING_LARGE_CONFIG).unwrap())]
#[case(RepoObject::open(*ZPAQ_PACKING_CONFIG).unwrap())]
pub fn object_config(#[case] repo_object: RepoObject) {}
