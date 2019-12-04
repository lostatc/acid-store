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

use crate::block::BlockAddress;

/// An object in the archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveObject {
    /// The metadata associated with this obbject.
    pub metadata: HashMap<String, Vec<u8>>,

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
