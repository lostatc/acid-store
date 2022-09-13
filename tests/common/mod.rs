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
pub use spectral::prelude::*;
#[cfg(feature = "store-directory")]
pub use store::{directory_config, directory_store};
pub use store::{memory_config, memory_store};
#[cfg(feature = "store-rclone")]
pub use store::{rclone_config, rclone_store};
#[cfg(feature = "store-redis")]
pub use store::{redis_config, redis_store};
#[cfg(feature = "store-s3")]
pub use store::{s3_config, s3_store};
#[cfg(feature = "store-sftp")]
pub use store::{sftp_config, sftp_store};
#[cfg(feature = "store-sqlite")]
pub use store::{sqlite_config, sqlite_store};
