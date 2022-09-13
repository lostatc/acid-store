use std::fmt::{self, Debug, Formatter};

use static_assertions::assert_obj_safe;

uuid_type! {
    /// The UUID of a block in a [`DataStore`].
    ///
    /// [`DataStore`]: crate::store::DataStore
    BlockId
}

/// A key for accessing a block in a [`DataStore`].
///
/// [`DataStore`]: crate::store::DataStore
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockKey {
    Data(BlockId),
    Lock(BlockId),
    Header(BlockId),
    Super,
    Version,
}

/// A type of block in a [`DataStore`].
///
/// [`DataStore`]: crate::store::DataStore
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlockType {
    Data,
    Lock,
    Header,
}

/// A persistent store for blocks of data.
///
/// A `DataStore` persistently stores blocks of data uniquely identified by [`BlockKey`] values.
/// Data stores ar used as the storage backend for repositories in the [`crate::repo`] module.
///
/// [`BlockKey`]: crate::store::BlockKey
pub trait DataStore: Send {
    /// Write the given `data` as a new block with the given `key`.
    ///
    /// If this method returns `Ok`, the block is stored persistently until it is removed with
    /// `remove_block`. If this method returns `Err`, the block is not stored persistently
    /// and it is up to the implementation to ensure that any data which may have been written is
    /// cleaned up.
    ///
    /// If a block with the given `key` already exists, it is overwritten.
    ///
    /// This is an atomic operation.
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> anyhow::Result<()>;

    /// Return the bytes of the block with the given `key`.
    ///
    /// If there is no block with the given `key`, return `None`.
    fn read_block(&mut self, key: BlockKey) -> anyhow::Result<Option<Vec<u8>>>;

    /// Remove the block with the given `key` from the store.
    ///
    /// If this method returns `Ok`, the given `key` is no longer stored persistently and any space
    /// allocated for it will be freed. If this method returns `Err`, the block is still stored
    /// persistently.
    ///
    /// If there is no block with the given `key`, this method does nothing and returns `Ok`.
    ///
    /// This is an atomic operation.
    fn remove_block(&mut self, key: BlockKey) -> anyhow::Result<()>;

    /// Return a list of IDs of blocks of the given `kind` in the store.
    fn list_blocks(&mut self, kind: BlockType) -> anyhow::Result<Vec<BlockId>>;
}

assert_obj_safe!(DataStore);

impl DataStore for Box<dyn DataStore> {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        self.as_mut().write_block(key, data)
    }

    fn read_block(&mut self, key: BlockKey) -> anyhow::Result<Option<Vec<u8>>> {
        self.as_mut().read_block(key)
    }

    fn remove_block(&mut self, key: BlockKey) -> anyhow::Result<()> {
        self.as_mut().remove_block(key)
    }

    fn list_blocks(&mut self, kind: BlockType) -> anyhow::Result<Vec<BlockId>> {
        self.as_mut().list_blocks(kind)
    }
}

impl Debug for dyn DataStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("DataStore")
    }
}
