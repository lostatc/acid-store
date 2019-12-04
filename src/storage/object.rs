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

use super::block::BlockAddress;

/// An object in an archive.
///
/// An `ArchiveObject` has `metadata` and `data` associated with it. An object's `metadata` must be
/// small enough to be held in memory, while an object's data can be directly read from and written
/// to an `Archive`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveObject {
    /// The metadata associated with this object.
    pub metadata: Vec<u8>,

    /// A handle for accessing the data associated with this object.
    pub data: Option<DataHandle>,
}

impl ArchiveObject {
    pub fn new() -> Self {
        ArchiveObject {
            metadata: HashMap::new(),
            data: None,
        }
    }
}

impl Default for ArchiveObject {
    fn default() -> Self {
        Self::new()
    }
}

/// A handle for accessing the data associated with an object.
///
/// A `DataHandle` does not store the data itself, but contains a reference to data stored in an
/// archive file. Values of this type can be cloned to allow two or more objects to share the same
/// data; two `DataHandle` values are equal when they reference the same data. When a `DataHandle`
/// is not owned by any `ArchiveObject`, the data it references can be overwritten by new data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataHandle {
    /// The size of the object's data in bytes.
    pub(super) size: u64,

    /// The addresses of the blocks containing the object's data.
    pub(super) blocks: Vec<BlockAddress>,
}

impl DataHandle {
    /// The size of the object's data in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }
}
