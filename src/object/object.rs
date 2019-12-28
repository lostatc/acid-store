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

use blake2::digest::{Input, VariableOutput};
use blake2::VarBlake2b;
use serde::{Deserialize, Serialize};

use super::hashing::Checksum;

/// The size of the checksums used for uniquely identifying chunks.
pub const CHUNK_HASH_SIZE: usize = 32;

/// A 256-bit checksum used for uniquely identifying a chunk.
pub type ChunkHash = [u8; CHUNK_HASH_SIZE];

/// Compute the BLAKE2 checksum of the given `data` and return the result.
pub fn chunk_hash(data: &[u8]) -> ChunkHash {
    let mut hasher = VarBlake2b::new(CHUNK_HASH_SIZE).unwrap();
    hasher.input(data);
    let mut checksum = [0u8; CHUNK_HASH_SIZE];
    hasher.variable_result(|result| checksum.copy_from_slice(result));
    checksum
}

/// A handle for accessing data in a repository.
///
/// An `Object` doesn't own or store data itself, but references data stored in a repository.
///
/// If two objects from the same repository are equal, they represent the same underlying data.
/// Comparisons between objects from different repositories are meaningless.
///
/// An object can be cloned to create multiple handles for accessing the same data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Object {
    /// The original size of the data in bytes.
    pub(super) size: u64,

    /// The self-describing checksum of the data.
    pub(super) checksum: Checksum,

    /// The checksums of the chunks which make up the data.
    pub(super) chunks: Vec<ChunkHash>,
}

impl Object {
    /// The size of the data in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The checksum of the data.
    ///
    /// This does not compute the checksum, but returns a stored checksum.
    pub fn checksum(&self) -> &Checksum {
        &self.checksum
    }
}
