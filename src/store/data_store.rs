use std::fmt;

use static_assertions::assert_obj_safe;

use super::error::Result;

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
/// Data stores are used as the storage backend for repositories in the [`crate::repo`] module.
///
/// [`BlockKey`]: crate::store::BlockKey
pub trait DataStore: fmt::Debug + Send {
    /// Write the given `data` as a new block with the given `key`.
    ///
    /// If this method returns `Ok`, the block is stored persistently until it is removed with
    /// `remove_block`. If this method returns `Err`, the block is not stored persistently and it is
    /// up to the implementation to ensure that any data which may have been written is cleaned up.
    ///
    /// If a block with the given `key` already exists, it is overwritten.
    ///
    /// This is an atomic operation.
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> Result<()>;

    /// Return the bytes of the block with the given `key`.
    ///
    /// This appends the bytes of the block with the given `key` to `buf` and returns the number of
    /// bytes read if successful. If there is no block with the given `key`, this returns
    /// `Ok(None)`.
    ///
    /// If this method returns before all the bytes of the block are written to `buf`, then it must
    /// return `Err`.
    fn read_block(&mut self, key: BlockKey, buf: &mut Vec<u8>) -> Result<Option<usize>>;

    /// Remove the block with the given `key` from the store.
    ///
    /// If this method returns `Ok`, the given `key` is no longer stored persistently and any space
    /// allocated for it will be freed. If this method returns `Err`, the block is still stored
    /// persistently.
    ///
    /// If there is no block with the given `key`, this method does nothing and returns `Ok`.
    ///
    /// This is an atomic operation.
    fn remove_block(&mut self, key: BlockKey) -> Result<()>;

    /// Return a list of IDs of blocks of the given `kind` in the store.
    ///
    /// This appends the [`BlockId`]s to `list`.
    ///
    /// [`BlockId`]: crate::store::BlockId
    fn list_blocks(&mut self, kind: BlockType, list: &mut Vec<BlockId>) -> Result<()>;
}

assert_obj_safe!(DataStore);

impl DataStore for Box<dyn DataStore> {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> Result<()> {
        self.as_mut().write_block(key, data)
    }

    fn read_block(&mut self, key: BlockKey, buf: &mut Vec<u8>) -> Result<Option<usize>> {
        self.as_mut().read_block(key, buf)
    }

    fn remove_block(&mut self, key: BlockKey) -> Result<()> {
        self.as_mut().remove_block(key)
    }

    fn list_blocks(&mut self, kind: BlockType, list: &mut Vec<BlockId>) -> Result<()> {
        self.as_mut().list_blocks(kind, list)
    }
}