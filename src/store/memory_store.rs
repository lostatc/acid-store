use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::data_store::{BlockId, BlockKey, BlockType, DataStore};
use super::open_store::OpenStore;

#[derive(Debug, Clone, Default)]
struct BlockMap {
    data: HashMap<BlockId, Vec<u8>>,
    locks: HashMap<BlockId, Vec<u8>>,
    headers: HashMap<BlockId, Vec<u8>>,
    superblock: Option<Vec<u8>>,
    version: Option<Vec<u8>>,
}

/// The configuration for opening a [`MemoryStore`].
///
/// [`MemoryStore`]: crate::store::MemoryStore
#[derive(Debug, Clone, Default)]
pub struct MemoryConfig(Arc<Mutex<BlockMap>>);

impl MemoryConfig {
    /// Create a new empty `MemoryConfig`.
    pub fn new() -> Self {
        MemoryConfig(Arc::new(Mutex::new(BlockMap::default())))
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
    blocks: Arc<Mutex<BlockMap>>,
}

impl DataStore for MemoryStore {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        let mut block_map = self.blocks.lock().unwrap();
        match key {
            BlockKey::Data(id) => {
                block_map.data.insert(id, data.to_owned());
            }
            BlockKey::Lock(id) => {
                block_map.locks.insert(id, data.to_owned());
            }
            BlockKey::Header(id) => {
                block_map.headers.insert(id, data.to_owned());
            }
            BlockKey::Super => {
                block_map.superblock = Some(data.to_owned());
            }
            BlockKey::Version => {
                block_map.version = Some(data.to_owned());
            }
        }
        Ok(())
    }

    fn read_block(&mut self, key: BlockKey) -> anyhow::Result<Option<Vec<u8>>> {
        let block_map = self.blocks.lock().unwrap();
        Ok(match key {
            BlockKey::Data(id) => block_map.data.get(&id).map(|data| data.to_owned()),
            BlockKey::Lock(id) => block_map.locks.get(&id).map(|data| data.to_owned()),
            BlockKey::Header(id) => block_map.headers.get(&id).map(|data| data.to_owned()),
            BlockKey::Super => block_map.superblock.clone(),
            BlockKey::Version => block_map.version.clone(),
        })
    }

    fn remove_block(&mut self, key: BlockKey) -> anyhow::Result<()> {
        let mut block_map = self.blocks.lock().unwrap();
        match key {
            BlockKey::Data(id) => {
                block_map.data.remove(&id);
            }
            BlockKey::Lock(id) => {
                block_map.locks.remove(&id);
            }
            BlockKey::Header(id) => {
                block_map.headers.remove(&id);
            }
            BlockKey::Super => {
                block_map.superblock = None;
            }
            BlockKey::Version => {
                block_map.version = None;
            }
        }
        Ok(())
    }

    fn list_blocks(&mut self, kind: BlockType) -> anyhow::Result<Vec<BlockId>> {
        let block_map = self.blocks.lock().unwrap();
        Ok(match kind {
            BlockType::Data => block_map.data.keys().copied().collect(),
            BlockType::Lock => block_map.locks.keys().copied().collect(),
            BlockType::Header => block_map.headers.keys().copied().collect(),
        })
    }
}
