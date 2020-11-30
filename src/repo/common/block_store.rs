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

use std::cmp::min;

use uuid::Uuid;

use super::state::{Pack, PackIndex, RepoState};

/// Encode and decode blocks of data.
pub trait BlockEncoder {
    /// Compress and encrypt the given `data` and return it.
    fn encode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>>;

    /// Decrypt and decompress the given `data` and return it.
    fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>>;
}

impl BlockEncoder for RepoState {
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

/// Read and decode blocks of data.
pub trait BlockReader {
    /// Return the bytes of the block with the given `id`.
    ///
    /// The data is decoded before it is returned.
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>>;
}

/// Encode and write blocks of data.
pub trait BlockWriter {
    /// Write the given `data` as a new block with the given `id`.
    ///
    /// The data is encoded before it is written.
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()>;
}

/// A block store which packs data into fixed-size blocks.
pub struct PackingBlockStore<'a> {
    state: &'a mut RepoState,
    pack_size: u32,
}

impl<'a> PackingBlockStore<'a> {
    pub fn new(state: &'a mut RepoState, pack_size: u32) -> Self {
        PackingBlockStore { state, pack_size }
    }
}

impl BlockReader for PackingBlockStore {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let index_list = match self.state.packs.get(&id) {
            Some(pack_index) => pack_index,
            None => return Err(crate::Error::InvalidData),
        };

        let block_size = index_list.iter().map(|index| index.size).sum();
        let mut block_buffer = Vec::with_capacity(block_size as usize);

        // A block can be spread across multiple packs. Get the data from each pack and concatenate
        // them.
        for pack_index in index_list {
            // Check if the data we need is already in the read buffer.
            let pack_buffer = match &self.state.read_buffer {
                // Read the data from the read buffer.
                Some(pack) if pack.id == pack_index.id => &pack.buffer,

                // Read a new pack into the read buffer.
                _ => {
                    let encoded_pack_buffer = self
                        .state
                        .store
                        .lock()
                        .unwrap()
                        .read_block(pack_index.id)?
                        .map_err(|error| crate::Store::Error(error))?
                        .ok_or(crate::Error::InvalidData)?;
                    let pack_buffer = self.state.decode_data(encoded_pack_buffer.as_slice())?;
                    let pack = Pack {
                        id: pack_index.id,
                        buffer: pack_buffer,
                    };
                    self.state.read_buffer = Some(pack);
                    self.state.read_buffer.unwrap().buffer
                }
            };

            // Get the slice of the pack containing the block data.
            let start = pack_index.offset;
            let end = pack_index.offset + pack_index.size;
            block_buffer.extend_from_slice(&pack_buffer[start..end]);
        }

        Ok(block_buffer)
    }
}

impl BlockWriter for PackingBlockStore {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()> {
        let current_pack = self
            .state
            .write_buffer
            .get_or_insert_with(|| Pack::new(self.pack_size));

        // The block's offset from the start of the current pack.
        let mut current_offset = current_pack.buffer.len() as u32;

        // The size of the block being written in the current pack.
        let mut current_size = 0u32;

        // The number of bytes written in from `data`.
        let mut bytes_written = 0usize;

        // The amount of space remaining in the current pack.
        let mut remaining_space;

        // The end index of the bytes to write from `data`.
        let mut buffer_end;

        // The slice of `data` to write to the current pack.
        let mut next_buffer;

        loop {
            // Fill the current pack with the provided `data`.
            remaining_space = self.pack_size as usize - current_pack.buffer.len();
            buffer_end = min(bytes_written + remaining_space, data.len());
            next_buffer = &data[bytes_written..buffer_end];
            current_pack.buffer.extend_from_slice(next_buffer);
            bytes_written += next_buffer.len();
            current_size += next_buffer.len();

            assert!(
                current_pack.buffer.len() <= self.pack_size as usize,
                "The size of the current pack has exceeded the configured pack size."
            );

            assert!(
                bytes_written <= data.len(),
                "More bytes were written than are available in the provided buffer."
            );

            // Write the location of this block in the pack.
            let pack_index = PackIndex {
                id: current_pack.id,
                offset: current_offset,
                size: current_size,
            };
            self.state
                .packs
                .entry(id)
                .or_insert_with(Vec::new)
                .push(pack_index);

            // If we've filled the current pack, write it to the data store.
            if current_pack.buffer.len() == self.pack_size {
                let encoded_pack = self.state.encode_data(current_pack.buffer.as_slice())?;
                self.state
                    .store
                    .lock()
                    .unwrap()
                    .write_block(current_pack.id, encoded_pack.as_slice())
                    .map_err(|error| crate::Error::Store(error))?;

                // We're starting a new pack, so these need to be reset.
                current_offset = 0;
                current_size = 0;

                *current_pack = Pack::new(self.pack_size);
            }

            // Break once we've written all the `data`.
            if bytes_written == data.len() {
                break;
            }
        }

        Ok(())
    }
}

/// A block store which writes blocks directly to the backing data store.
pub struct DirectBlockStore<'a> {
    state: &'a mut RepoState,
}

impl<'a> DirectBlockStore<'a> {
    pub fn new(state: &'a mut RepoState) -> Self {
        DirectBlockStore { state }
    }
}

impl BlockReader for DirectBlockStore {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let encoded_block = self
            .state
            .store
            .lock()
            .unwrap()
            .read_block(id)
            .map_err(|error| crate::Error::Store(error))?
            .ok_or(crate::Error::InvalidData)?;
        self.state.decode_data(encoded_block.as_slice())
    }
}

impl BlockWriter for DirectBlockStore {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()> {
        let encoded_block = self.state.encode_data(data)?;
        self.state
            .store
            .lock()
            .unwrap()
            .write_block(id, encoded_block.as_slice())
            .map_err(|error| crate::Error::Store(error))
    }
}
