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

#![allow(dead_code)]
#![cfg(all(feature = "encryption", feature = "compression"))]

mod assertions;
mod config;
mod data;
mod repository;
mod store;

pub use assertions::ErrorVariantAssertions;
pub use config::{
    encoding_config, fixed_config, fixed_packing_large_config, fixed_packing_small_config,
    zpaq_config, zpaq_packing_config,
};
pub use data::{buffer, fixed_buffer, larger_buffer, smaller_buffer, temp_dir};
pub use repository::{create_repo, repo, repo_object, repo_store, RepoObject, RepoStore};
pub use rstest::*;
pub use serial_test::serial;
pub use spectral::prelude::*;
#[cfg(feature = "store-directory")]
pub use store::directory_store;
pub use store::memory_store;
#[cfg(feature = "store-rclone")]
pub use store::rclone_store;
#[cfg(feature = "store-redis")]
pub use store::redis_store;
#[cfg(feature = "store-s3")]
pub use store::s3_store;
#[cfg(feature = "store-sftp")]
pub use store::sftp_store;
#[cfg(feature = "store-sqlite")]
pub use store::sqlite_store;
