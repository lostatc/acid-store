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
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::block::{BlockAddress, Checksum};

/// Information about an entry in the archive.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveEntry {
    /// The name of the entry.
    pub name: String,

    /// The metadata associated with this entry.
    pub metadata: HashMap<String, Vec<u8>>,

    /// The data associated with this entry.
    pub(super) data: Option<EntryData>,
}

impl ArchiveEntry {
    /// The data associated with this entry, or `None` if there is none.
    pub fn data(&self) -> Option<EntryData> {
        self.data
    }
}

/// Information about the data associated with an entry.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryData {
    /// The size of the entry's data in bytes.
    pub(super) size: u64,

    /// The 256-bit BLAKE2 checksum of the entry's data.
    pub(super) checksum: Checksum,

    /// A reader for reading the entry's data from the archive.
    pub(super) blocks: Vec<BlockAddress>,
}

impl EntryData {
    /// The size of the entry's data in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The 256-bit BLAKE2 checksum of the entry's data.
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }
}
