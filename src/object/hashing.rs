/*
 * Copyright 2019 Garrett Powell
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

use blake2::{Blake2b, Blake2s};
use digest::{Digest, DynDigest};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Sha512};
use sha3::{Keccak256, Keccak512, Sha3_256, Sha3_512};

/// A hash algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HashAlgorithm {
    /// SHA-2 (256 bits)
    Sha2_256,

    /// SHA-2 (512 bits)
    Sha2_512,

    /// SHA-3 (256 bits)
    Sha3_256,

    /// SHA-3 (512 bits)
    Sha3_512,

    /// Keccak (256 bits)
    Keccak256,

    /// Keccak (512 bits)
    Keccak512,

    /// BLAKE2s (256 bits)
    Blake2s256,

    /// BLAKE2b (512 bits)
    Blake2b512,
}

impl HashAlgorithm {
    /// Returns a digest for computing a checksum using this algorithm.
    pub(super) fn digest(&self) -> Box<dyn DynDigest> {
        match self {
            HashAlgorithm::Sha2_256 => Box::new(Sha256::new()),
            HashAlgorithm::Sha2_512 => Box::new(Sha512::new()),
            HashAlgorithm::Sha3_256 => Box::new(Sha3_256::new()),
            HashAlgorithm::Sha3_512 => Box::new(Sha3_512::new()),
            HashAlgorithm::Keccak256 => Box::new(Keccak256::new()),
            HashAlgorithm::Keccak512 => Box::new(Keccak512::new()),
            HashAlgorithm::Blake2s256 => Box::new(Blake2s::new()),
            HashAlgorithm::Blake2b512 => Box::new(Blake2b::new()),
        }
    }

    /// The size of this algorithm's digest in bytes.
    pub fn output_size(&self) -> usize {
        match self {
            HashAlgorithm::Sha2_256 => 32,
            HashAlgorithm::Sha2_512 => 64,
            HashAlgorithm::Sha3_256 => 32,
            HashAlgorithm::Sha3_512 => 64,
            HashAlgorithm::Keccak256 => 32,
            HashAlgorithm::Keccak512 => 64,
            HashAlgorithm::Blake2s256 => 32,
            HashAlgorithm::Blake2b512 => 64,
        }
    }
}

/// A checksum of a piece of data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Checksum {
    /// The algorithm used to compute the checksum.
    pub algorithm: HashAlgorithm,

    /// The bytes of the checksum.
    pub digest: Vec<u8>,
}
