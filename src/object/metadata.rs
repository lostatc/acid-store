/*
 * Copyright 2019 Wren Powell
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

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{Compression, Encryption, HashAlgorithm, RepositoryConfig};
use super::encryption::KeySalt;

/// Metadata for a repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryMetadata<ID> {
    /// The unique ID of this repository.
    pub id: Uuid,

    /// The number of bits that define a chunk boundary.
    ///
    /// The average size of a chunk will be 2^`chunker_bits` bytes.
    pub chunker_bits: u32,

    /// The compression method being used in this repository.
    pub compression: Compression,

    /// The encryption method being used in this repository.
    pub encryption: Encryption,

    /// The hash algorithm used for computing object checksums.
    pub hash_algorithm: HashAlgorithm,

    /// The master encryption key encrypted with the user's password.
    pub master_key: Vec<u8>,

    /// The salt used to derive a key from the user's password.
    pub salt: KeySalt,

    /// The ID of the chunk which stores the repository's header.
    pub header: ID,
}

impl<ID> RepositoryMetadata<ID> {
    /// Return the config used to create this repository.
    pub fn to_config(&self) -> RepositoryConfig {
        RepositoryConfig {
            chunker_bits: self.chunker_bits,
            compression: self.compression,
            encryption: self.encryption,
            hash_algorithm: self.hash_algorithm,
        }
    }
}
