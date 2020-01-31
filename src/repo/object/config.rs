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

use super::compression::Compression;
use super::encryption::{Encryption, ResourceLimit};

/// The configuration for an repository.
///
/// This type is used to configure a repository when it is created. Once a repository is created,
/// the config values provided cannot be changed. This type implements `Default` to provide a
/// reasonable default configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryConfig {
    /// A value which determines the chunk size for content-defined deduplication.
    ///
    /// Data is deduplicated by splitting it into chunks. If two or more objects have a chunk in
    /// common, it will only be stored once. This value determines the average size of those chunks,
    /// which will be 2^`chunker_bits` bytes. Smaller chunks will generally result in better
    /// deduplication ratios and thus a smaller repository, but may hurt performance. Chunks that
    /// are too small may result in worse deduplication rations due to the overhead of compression
    /// and encryption.
    ///
    /// The default value is `20` (1MiB average chunk size).
    pub chunker_bits: u32,

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

impl Default for RepositoryConfig {
    fn default() -> Self {
        RepositoryConfig {
            chunker_bits: 20,
            compression: Compression::None,
            encryption: Encryption::None,
            memory_limit: ResourceLimit::Interactive,
            operations_limit: ResourceLimit::Interactive,
        }
    }
}
