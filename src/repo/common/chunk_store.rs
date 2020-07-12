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

use std::collections::HashSet;

use uuid::Uuid;

use crate::store::DataStore;

use super::id_table::UniqueId;
use super::object::{chunk_hash, Chunk};
use super::state::{ChunkInfo, RepoState};

/// Encode and decode chunks of data.
pub trait ChunkEncoder {
    /// Compress and encrypt the given `data` and return it.
    fn encode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>>;

    /// Decrypt and decompress the given `data` and return it.
    fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>>;
}

impl<S: DataStore> ChunkEncoder for RepoState<S> {
    fn encode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let compressed_data = self.metadata.compression.compress(data)?;

        Ok(self
            .metadata
            .encryption
            .encrypt(compressed_data.as_slice(), &self.master_key))
    }

    fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let decrypted_data = self.metadata.encryption.decrypt(data, &self.master_key)?;

        Ok(self
            .metadata
            .compression
            .decompress(decrypted_data.as_slice())?)
    }
}

/// Read chunks of data.
pub trait ChunkReader {
    /// Return the bytes of the chunk with the given checksum.
    fn read_chunk(&self, chunk: Chunk) -> crate::Result<Vec<u8>>;
}

impl<S: DataStore> ChunkReader for RepoState<S> {
    fn read_chunk(&self, chunk: Chunk) -> crate::Result<Vec<u8>> {
        let chunk_info = self.chunks.get(&chunk).ok_or(crate::Error::InvalidData)?;
        let chunk = self
            .store
            .lock()
            .unwrap()
            .read_block(chunk_info.block_id)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .ok_or(crate::Error::InvalidData)?;

        self.decode_data(chunk.as_slice())
    }
}

/// Write chunks of data.
pub trait ChunkWriter {
    /// Write the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum may be returned without
    /// writing any new data.
    ///
    /// This requires a unique `id` which is used for reference counting.
    fn write_chunk(&mut self, data: &[u8], id: UniqueId) -> crate::Result<Chunk>;
}

impl<S: DataStore> ChunkWriter for RepoState<S> {
    fn write_chunk(&mut self, data: &[u8], id: UniqueId) -> crate::Result<Chunk> {
        // Get a checksum of the unencoded data.
        let chunk = Chunk {
            hash: chunk_hash(data),
            size: data.len(),
        };

        // Check if the chunk already exists.
        if let Some(chunk_info) = self.chunks.get_mut(&chunk) {
            chunk_info.references.insert(id);
            return Ok(chunk);
        }

        // Encode the data.
        let encoded_data = self.encode_data(data)?;
        let block_id = Uuid::new_v4();

        // Write the data to the data store.
        self.store
            .lock()
            .unwrap()
            .write_block(block_id, &encoded_data)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        // Add the chunk to the header.
        let chunk_info = ChunkInfo {
            block_id,
            references: {
                let mut id_set = HashSet::new();
                id_set.insert(id);
                id_set
            },
        };
        self.chunks.insert(chunk, chunk_info);

        Ok(chunk)
    }
}
