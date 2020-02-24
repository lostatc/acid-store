/*
 * Copyright 2019-2020 Garrett Powell
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

use std::cell::{Ref, RefCell, RefMut};

use uuid::Uuid;

use crate::repo::Key;
use crate::store::DataStore;

use super::object::{chunk_hash, Chunk};
use super::state::ObjectState;

/// A wrapper over a `DataStore` which adds support for encryption and compression.
///
/// This type allows for reading and writing chunks, which are independently compressed and
/// encrypted blobs of data which are identified by their checksum.
#[derive(Debug)]
pub struct ChunkStore<'a, K: Key, S: DataStore>(&'a RefCell<ObjectState<K, S>>);

impl<'a, K: Key, S: DataStore> ChunkStore<'a, K, S> {
    /// Create a new instance of `ChunkStore`.
    pub fn new(state: &'a RefCell<ObjectState<K, S>>) -> Self {
        Self(state)
    }

    /// Borrow the repository's state immutably.
    ///
    /// The purpose of this method is to enforce safe usage of the `RefCell` using references.
    fn borrow_state(&self) -> Ref<ObjectState<K, S>> {
        self.0.borrow()
    }

    /// Borrow the repository's state mutably.
    ///
    /// The purpose of this method is to enforce safe usage of the `RefCell` using references.
    fn borrow_state_mut(&mut self) -> RefMut<ObjectState<K, S>> {
        self.0.borrow_mut()
    }

    /// Compress and encrypt the given `data` and return it.
    pub fn encode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let state = self.borrow_state();
        let compressed_data = state.metadata.compression.compress(data)?;

        Ok(state
            .metadata
            .encryption
            .encrypt(compressed_data.as_slice(), &state.master_key))
    }

    /// Decrypt and decompress the given `data` and return it.
    pub fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let state = self.borrow_state();
        let decrypted_data = state.metadata.encryption.decrypt(data, &state.master_key)?;

        Ok(state
            .metadata
            .compression
            .decompress(decrypted_data.as_slice())?)
    }

    /// Write the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum may be returned without
    /// writing any new data.
    pub fn write_chunk(&mut self, data: &[u8]) -> crate::Result<Chunk> {
        let state = self.borrow_state();

        // Get a checksum of the unencoded data.
        let chunk = Chunk {
            hash: chunk_hash(data),
            size: data.len(),
        };

        // Check if the chunk already exists.
        if state.header.chunks.contains_key(&chunk) {
            return Ok(chunk);
        }

        // Encode the data.
        let encoded_data = self.encode_data(data)?;
        let block_id = Uuid::new_v4();

        drop(state);
        let mut state = self.borrow_state_mut();

        // Write the data to the data store.
        state
            .store
            .write_block(block_id, &encoded_data)
            .map_err(anyhow::Error::from)?;

        // Add the chunk to the header.
        state.header.chunks.insert(chunk, block_id);

        Ok(chunk)
    }

    /// Return the bytes of the chunk with the given checksum.
    pub fn read_chunk(&mut self, chunk: Chunk) -> crate::Result<Vec<u8>> {
        let mut state = self.borrow_state_mut();

        let chunk_id = *state
            .header
            .chunks
            .get(&chunk)
            .ok_or(crate::Error::InvalidData)?;
        let chunk = state
            .store
            .read_block(chunk_id)
            .map_err(anyhow::Error::from)?
            .ok_or(crate::Error::InvalidData)?;

        drop(state);

        self.decode_data(chunk.as_slice())
    }
}
