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

use std::io::Read;

use blake2::{VarBlake2b, VarBlake2s};
use digest::{Digest, FixedOutput, Input, VariableOutput};
use serde::{Deserialize, Serialize};
use sha2::{Sha224, Sha256, Sha384, Sha512, Sha512Trunc224, Sha512Trunc256};
use sha3::{Sha3_224, Sha3_256, Sha3_384, Sha3_512};

/// The size of the buffer to use when copying bytes.
const BUFFER_SIZE: usize = 4096;

/// A simple digest which supports variable-size output.
///
/// We need this trait because `digest::Digest` does not support variable-sized output.
pub trait SimpleDigest {
    fn input(&mut self, data: &[u8]);

    fn result(self: Box<Self>) -> Vec<u8>;
}

struct FixedDigest<T: Input + FixedOutput>(T);

impl<T: Input + FixedOutput> SimpleDigest for FixedDigest<T> {
    fn input(&mut self, data: &[u8]) {
        self.0.input(data)
    }

    fn result(self: Box<Self>) -> Vec<u8> {
        self.0.fixed_result().to_vec()
    }
}

struct VariableDigest<T: Input + VariableOutput>(T);

impl<T: Input + VariableOutput> SimpleDigest for VariableDigest<T> {
    fn input(&mut self, data: &[u8]) {
        self.0.input(data)
    }

    fn result(self: Box<Self>) -> Vec<u8> {
        self.0.vec_result()
    }
}

/// A cryptographic hash algorithm.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
#[non_exhaustive]
pub enum HashAlgorithm {
    /// SHA-224
    Sha224,

    /// SHA-256
    Sha256,

    /// SHA-384
    Sha384,

    /// SHA-512
    Sha512,

    /// SHA-512/224
    Sha512Trunc224,

    /// SHA-512/256
    Sha512Trunc256,

    /// SHA3-224
    Sha3_224,

    /// SHA3-256
    Sha3_256,

    /// SHA3-384
    Sha3_384,

    /// SHA3-512
    Sha3_512,

    /// BLAKE2b
    ///
    /// This accepts a digest size in the range of 1-64 bytes.
    Blake2b(usize),

    /// BLAKE2s
    ///
    /// This accepts a digest size in the range of 1-32 bytes.
    Blake2s(usize),
}

impl HashAlgorithm {
    /// The output size of the hash algorithm in bytes.
    pub fn output_size(&self) -> usize {
        match self {
            HashAlgorithm::Sha224 => Sha224::output_size(),
            HashAlgorithm::Sha256 => Sha256::output_size(),
            HashAlgorithm::Sha384 => Sha384::output_size(),
            HashAlgorithm::Sha512 => Sha512::output_size(),
            HashAlgorithm::Sha512Trunc224 => Sha512Trunc224::output_size(),
            HashAlgorithm::Sha512Trunc256 => Sha512Trunc256::output_size(),
            HashAlgorithm::Sha3_224 => Sha3_224::output_size(),
            HashAlgorithm::Sha3_256 => Sha3_256::output_size(),
            HashAlgorithm::Sha3_384 => Sha3_384::output_size(),
            HashAlgorithm::Sha3_512 => Sha3_512::output_size(),
            HashAlgorithm::Blake2b(size) => *size,
            HashAlgorithm::Blake2s(size) => *size,
        }
    }

    /// Compute and return the hash of the given `data` using this hash algorithm.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn hash(&self, mut data: impl Read) -> crate::Result<Vec<u8>> {
        let mut buffer = [0u8; BUFFER_SIZE];
        let mut digest = self.digest();
        let mut bytes_read;

        loop {
            bytes_read = data.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            digest.input(&buffer[..bytes_read]);
        }

        Ok(digest.result())
    }

    pub(super) fn digest(&self) -> Box<dyn SimpleDigest> {
        match self {
            HashAlgorithm::Sha224 => Box::new(FixedDigest(Sha224::default())),
            HashAlgorithm::Sha256 => Box::new(FixedDigest(Sha256::default())),
            HashAlgorithm::Sha384 => Box::new(FixedDigest(Sha384::default())),
            HashAlgorithm::Sha512 => Box::new(FixedDigest(Sha512::default())),
            HashAlgorithm::Sha512Trunc224 => Box::new(FixedDigest(Sha512Trunc224::default())),
            HashAlgorithm::Sha512Trunc256 => Box::new(FixedDigest(Sha512Trunc256::default())),
            HashAlgorithm::Sha3_224 => Box::new(FixedDigest(Sha3_224::default())),
            HashAlgorithm::Sha3_256 => Box::new(FixedDigest(Sha3_256::default())),
            HashAlgorithm::Sha3_384 => Box::new(FixedDigest(Sha3_384::default())),
            HashAlgorithm::Sha3_512 => Box::new(FixedDigest(Sha3_512::default())),
            HashAlgorithm::Blake2b(size) => Box::new(VariableDigest(
                VarBlake2b::new(*size).expect("Invalid digest size for BLAKE2b."),
            )),
            HashAlgorithm::Blake2s(size) => Box::new(VariableDigest(
                VarBlake2s::new(*size).expect("Invalid digest size for BLAKE2s."),
            )),
        }
    }
}
