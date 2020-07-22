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

use std::collections::HashMap;
use std::convert::Infallible;
use uuid::Uuid;

use super::common::DataStore;

/// A `DataStore` which stores data in memory.
///
/// Unlike other `DataStore` implementations, data in a `MemoryStore` is not stored persistently
/// and is only accessible to the current process. This data store is useful for testing.
///
/// None of the methods in this data store will ever return `Err`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemoryStore {
    blocks: HashMap<Uuid, Vec<u8>>,
}

impl MemoryStore {
    /// Create a new empty `MemoryStore`.
    pub fn new() -> Self {
        MemoryStore {
            blocks: HashMap::new(),
        }
    }
}

impl DataStore for MemoryStore {
    type Error = Infallible;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        self.blocks.insert(id.to_owned(), data.to_owned());
        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.blocks.get(&id).map(|data| data.to_owned()))
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        self.blocks.remove(&id);
        Ok(())
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        Ok(self.blocks.keys().copied().collect())
    }
}
