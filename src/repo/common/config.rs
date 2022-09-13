use serde::{Deserialize, Serialize};

use super::chunking::Chunking;
use super::compression::Compression;
use super::encryption::{Encryption, ResourceLimit};
use super::packing::Packing;

/// The configuration for a repository.
///
/// This type is used to configure a repository when it is created. This type implements `Default`
/// to provide a reasonable default configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RepoConfig {
    /// The chunking method to use in the repository.
    ///
    /// The default value is `Chunking::FIXED`.
    pub chunking: Chunking,

    /// The packing method to use in the repository.
    ///
    /// The default value is `Packing::None`.
    pub packing: Packing,

    /// The compression method to use in the repository.
    ///
    /// The default value is `Compression::None`.
    pub compression: Compression,

    /// The encryption method to use in the repository.
    ///
    /// The default value is `Encryption::None`.
    pub encryption: Encryption,

    /// The maximum amount of memory key derivation will use if encryption is enabled.
    ///
    /// The default value is `ResourceLimit::Interactive`.
    pub memory_limit: ResourceLimit,

    /// The maximum number of computations key derivation will perform if encryption is enabled.
    ///
    /// The default value is `ResourceLimit::Interactive`.
    pub operations_limit: ResourceLimit,
}

impl Default for RepoConfig {
    fn default() -> Self {
        RepoConfig {
            chunking: Chunking::FIXED,
            packing: Packing::None,
            compression: Compression::None,
            encryption: Encryption::None,
            memory_limit: ResourceLimit::Interactive,
            operations_limit: ResourceLimit::Interactive,
        }
    }
}
