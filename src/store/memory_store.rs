/*
 * Copyright 2019-2021 Wren Powell
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
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use super::data_store::DataStore;
use super::open_store::OpenStore;

/// The configuration for opening a [`MemoryStore`].
///
/// [`MemoryStore`]: crate::store::MemoryStore
#[derive(Debug, Clone)]
pub struct MemoryConfig(Arc<Mutex<HashMap<Uuid, Vec<u8>>>>);

impl MemoryConfig {
    /// Create a new empty `MemoryConfig`.
    pub fn new() -> Self {
        MemoryConfig(Arc::new(Mutex::new(HashMap::new())))
    }
}

impl OpenStore for MemoryConfig {
    type Store = MemoryStore;

    fn open(&self) -> crate::Result<Self::Store> {
        Ok(MemoryStore {
            blocks: Arc::clone(&self.0),
        })
    }
}

/// A `DataStore` which stores data in memory.
///
/// Unlike other `DataStore` implementations, data in a `MemoryStore` is not stored persistently
/// and is only accessible to the current process. This data store is useful for testing.
///
/// None of the methods in this data store will ever return `Err`.
///
/// You can use [`MemoryConfig`] to open a data store of this type.
///
/// [`MemoryConfig`]: crate::store::MemoryConfig
#[derive(Debug)]
pub struct MemoryStore {
    blocks: Arc<Mutex<HashMap<Uuid, Vec<u8>>>>,
}

impl DataStore for MemoryStore {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> anyhow::Result<()> {
        self.blocks
            .lock()
            .unwrap()
            .insert(id.to_owned(), data.to_owned());
        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self
            .blocks
            .lock()
            .unwrap()
            .get(&id)
            .map(|data| data.to_owned()))
    }

    fn remove_block(&mut self, id: Uuid) -> anyhow::Result<()> {
        self.blocks.lock().unwrap().remove(&id);
        Ok(())
    }

    fn list_blocks(&mut self) -> anyhow::Result<Vec<Uuid>> {
        Ok(self.blocks.lock().unwrap().keys().copied().collect())
    }
}
