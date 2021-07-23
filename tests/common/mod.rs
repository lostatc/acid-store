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

mod config;
mod data;
mod repository;
mod store;

pub use config::{
    ENCODING_CONFIG, FIXED_CONFIG, FIXED_PACKING_LARGE_CONFIG, FIXED_PACKING_SMALL_CONFIG,
    ZPAQ_CONFIG, ZPAQ_PACKING_CONFIG,
};
pub use data::{buffer, larger_buffer, smaller_buffer};
pub use repository::{open_repo, repo, repo_object, RepoObject};
#[cfg(feature = "store-directory")]
pub use store::directory_store;
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
pub use store::{memory_store, WithTempDir};
