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

use uuid::Uuid;

use crate::repo::Key;
use crate::store::DataStore;

use super::object::{Chunk, chunk_hash};
use super::state::RepositoryState;

/// Encode and decode chunks of data.
pub trait ChunkEncoder {
    /// Compress and encrypt the given `data` and return it.
    fn encode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>>;

    /// Decrypt and decompress the given `data` and return it.
    fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>>;
}

impl<K: Key, S: DataStore> ChunkEncoder for RepositoryState<K, S> {
    fn encode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let compressed_data = self.metadata.compression.compress(data)?;

        Ok(self
            .metadata
            .encryption
            .encrypt(compressed_data.as_slice(), &self.master_key))
    }

    fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let decrypted_data = self
            .metadata
            .encryption
            .decrypt(data, &self.master_key)?;

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

impl<K: Key, S: DataStore> ChunkReader for RepositoryState<K, S> {
    fn read_chunk(&self, chunk: Chunk) -> crate::Result<Vec<u8>> {
        let chunk_id = *self
            .header
            .chunks
            .get(&chunk)
            .ok_or(crate::Error::InvalidData)?;
        let chunk = self
            .store
            .lock()
            .unwrap()
            .read_block(chunk_id)
            .map_err(anyhow::Error::from)?
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
    fn write_chunk(&mut self, data: &[u8]) -> crate::Result<Chunk>;
}

impl<K: Key, S: DataStore> ChunkWriter for RepositoryState<K, S> {
    fn write_chunk(&mut self, data: &[u8]) -> crate::Result<Chunk> {
        // Get a checksum of the unencoded data.
        let chunk = Chunk {
            hash: chunk_hash(data),
            size: data.len(),
        };

        // Check if the chunk already exists.
        if self.header.chunks.contains_key(&chunk) {
            return Ok(chunk);
        }

        // Encode the data.
        let encoded_data = self.encode_data(data)?;
        let block_id = Uuid::new_v4();

        // Write the data to the data store.
        self
            .store
            .lock()
            .unwrap()
            .write_block(block_id, &encoded_data)
            .map_err(anyhow::Error::from)?;

        // Add the chunk to the header.
        self.header.chunks.insert(chunk, block_id);

        Ok(chunk)
    }
}
