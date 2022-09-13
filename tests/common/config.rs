#![macro_use]

use rstest_reuse::{self, *};

use acid_store::repo::{Chunking, Compression, Encryption, Packing, RepoConfig};

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
#[case::fixed_size_chunking(fixed_config())]
#[case::encoding(encoding_config())]
#[case::zpaq_chunking(zpaq_config())]
#[case::small_pack_size(fixed_packing_small_config())]
#[case::large_pack_size(fixed_packing_large_config())]
#[case::zpaq_packing(zpaq_packing_config())]
pub fn config(#[case] config: RepoConfig) {}

/// A parameterized test template which provides several differently-configured repositories.
#[template]
#[rstest]
#[case::fixed_size_chunking(create_repo(fixed_config()).unwrap())]
#[case::encoding(create_repo(encoding_config()).unwrap())]
#[case::zpaq_chunking(create_repo(zpaq_config()).unwrap())]
#[case::small_pack_size(create_repo(fixed_packing_small_config()).unwrap())]
#[case::large_pack_size(create_repo(fixed_packing_large_config()).unwrap())]
#[case::zpaq_packing(create_repo(zpaq_packing_config()).unwrap())]
pub fn repo_config(#[case] repo: KeyRepo<String>) {}

/// A parameterized test template which provides several differently-configured `RepoObject` values.
#[template]
#[rstest]
#[case::fixed_size_chunking(RepoObject::new(fixed_config()).unwrap())]
#[case::encoding(RepoObject::new(encoding_config()).unwrap())]
#[case::zpaq_chunking(RepoObject::new(zpaq_config()).unwrap())]
#[case::small_pack_size(RepoObject::new(fixed_packing_small_config()).unwrap())]
#[case::large_pack_size(RepoObject::new(fixed_packing_large_config()).unwrap())]
#[case::zpaq_packing(RepoObject::new(zpaq_packing_config()).unwrap())]
pub fn object_config(#[case] repo_object: RepoObject) {}

/// A parameterized test template which provides several differently-configured `RepoStore` values.
#[template]
#[rstest]
#[case::fixed_size_chunking(RepoStore::new(fixed_config()))]
#[case::encoding(RepoStore::new(encoding_config()))]
#[case::zpaq_chunking(RepoStore::new(zpaq_config()))]
#[case::small_pack_size(RepoStore::new(fixed_packing_small_config()))]
#[case::large_pack_size(RepoStore::new(fixed_packing_large_config()))]
#[case::zpaq_packing(RepoStore::new(zpaq_packing_config()))]
pub fn store_config(#[case] repo_store: RepoStore) {}
