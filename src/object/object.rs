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

use blake2::digest::{Input, VariableOutput};
use blake2::VarBlake2b;
use serde::{Deserialize, Serialize};

/// The size of the checksums used for uniquely identifying data.
pub const CHECKSUM_SIZE: usize = 32;

/// A 256-bit checksum used for uniquely identifying data.
pub type Checksum = [u8; CHECKSUM_SIZE];

/// Compute the BLAKE2b checksum of the given `data` and return the result.
pub fn compute_checksum(data: &[u8]) -> Checksum {
    let mut hasher = VarBlake2b::new(CHECKSUM_SIZE).unwrap();
    hasher.input(data);
    let mut checksum = [0u8; CHECKSUM_SIZE];
    hasher.variable_result(|result| checksum.copy_from_slice(result));
    checksum
}

/// An object in an archive.
///
/// An object is a handle for accessing data in an archive. It doesn't own or store the data itself.
/// If two objects are equal, they represent the same underlying data. An object can be cloned to
/// create multiple handles for accessing the same data.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Object {
    /// The original size of the data in bytes.
    pub(super) size: u64,

    /// The checksums of the chunks which make up the data.
    pub(super) chunks: Vec<Checksum>,
}

impl Object {
    /// The size of the data in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }
}
