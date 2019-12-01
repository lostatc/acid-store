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
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::block::{BlockAddress, Checksum};

/// The 256-bit BLAKE2 checksum of an empty byte array.
const EMPTY_CHECKSUM: Checksum = [0x0e, 0x57, 0x51, 0xc0, 0x26, 0xe5, 0x43, 0xb2, 0xe8, 0xab, 0x2e, 0xb0, 0x60, 0x99, 0xda, 0xa1, 0xd1, 0xe5, 0xdf, 0x47, 0x77, 0x8f, 0x77, 0x87, 0xfa, 0xab, 0x45, 0xcd, 0xf1, 0x2f, 0xe3, 0xa8];

/// Information about an entry in the archive.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveEntry {
    /// The name of the entry.
    pub name: String,

    /// The metadata associated with this entry.
    pub metadata: HashMap<String, Vec<u8>>,

    /// A handle for accessing the data associated with this entry.
    pub data: Option<DataHandle>,
}

/// A handle for accessing the data associated with an entry.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataHandle {
    /// The size of the entry's data in bytes.
    pub(super) size: u64,

    /// The 256-bit BLAKE2 checksum of the entry's data.
    pub(super) checksum: Checksum,

    /// A reader for reading the entry's data from the archive.
    pub(super) blocks: Vec<BlockAddress>,
}

impl DataHandle {
    /// Create a new `DataHandle` which represents no data.
    pub fn new() -> Self {
        DataHandle { size: 0, checksum: EMPTY_CHECKSUM, blocks: Vec::new() }
    }

    /// The size of the entry's data in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The 256-bit BLAKE2 checksum of the entry's data.
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }
}

impl Default for DataHandle {
    fn default() -> Self {
        Self::new()
    }
}
