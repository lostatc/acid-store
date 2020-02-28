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

use std::error;

use uuid::Uuid;

use bitflags::bitflags;

/// A persistent store for blocks of data.
///
/// A `DataStore` persistently stores blocks of data uniquely identified by UUIDs. Data stores are
/// used as the storage backend for repositories in the `repo` module. Data stores do not need to
/// provide their own locking mechanisms to protect against concurrent access.
pub trait DataStore {
    /// The error type for this data store.
    type Error: error::Error + Send + Sync + 'static;

    /// Write the given `data` as a new block with the given `id`.
    ///
    /// If this method returns `Ok`, the block is stored persistently until it is removed with
    /// `remove_block`. If this method returns `Err` or panics, the block is not stored persistently
    /// and it is up to the implementation to ensure that any data which may have been written is
    /// cleaned up.
    ///
    /// If a block with the given `id` already exists, it is overwritten.
    ///
    /// This is an atomic operation.
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error>;

    /// Return the bytes of the block with the given `id`.
    ///
    /// If there is no block with the given `id`, return `None`.
    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Remove the block with the given `id` from the store.
    ///
    /// If this method returns `Ok`, the given `id` is no longer stored persistently and any space
    /// allocated for it will be freed. If this method returns `Err` or panics, the block is still
    /// stored persistently.
    ///
    /// If there is no block with the given `id`, this method does nothing and returns `Ok`.
    ///
    /// This is an atomic operation.
    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error>;

    /// Return a list of IDs of blocks in the store.
    ///
    /// This only lists the IDs of blocks which are stored persistently.
    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error>;
}

bitflags! {
    /// Options for opening a data store.
    pub struct OpenOption: u32 {
        /// Create the data store if it doesn't exist.
        const CREATE = 1;

        /// Create the data store, failing if it already exists.
        ///
        /// `CREATE` and `TRUNCATE` are ignored if this is used.
        const CREATE_NEW = 2;

        /// Delete all blocks in the data store before opening it.
        const TRUNCATE = 4;
    }
}

/// A resource which can be opened.
pub trait Open {
    /// The type of the configuration used to open a resource.
    type Config;

    /// Open this resource using the given `config` and open `options`.
    ///
    /// # Errors
    /// - `Error::NotFound`: The resource does not exist and `OpenOption::CREATE` and
    /// `OpenOption::CREATE_NEW` were not passed.
    /// - `Error::UnsupportedFormat`: The resource exists but is an unsupported format.
    /// - `Error::AlreadyExists`: The resource already exists and `OpenOption::CREATE_NEW` was
    /// passed.
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Store`: An error occurred with the underlying data store.
    fn open(config: Self::Config, options: OpenOption) -> crate::Result<Self>
    where
        Self: Sized;
}
