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
use std::collections::HashSet;

use uuid::Uuid;

use crate::repo::common::packing::Packing;

use super::id_table::UniqueId;
use super::object::{chunk_hash, Chunk};
use super::state::{ChunkInfo, Pack, PackIndex, RepoState};

/// Encode and decode blocks of data.
pub trait EncodeBlock {
    /// Compress and encrypt the given `data` and return it.
    fn encode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>>;

    /// Decrypt and decompress the given `data` and return it.
    fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>>;
}

impl EncodeBlock for RepoState {
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
pub trait ReadBlock {
    /// Return the bytes of the block with the given `id`.
    ///
    /// The data is decoded before it is returned.
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>>;
}

/// Encode and write blocks of data.
pub trait WriteBlock {
    /// Write the given `data` as a new block with the given `id`.
    ///
    /// The data is encoded before it is written.
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()>;
}

/// A `ReadBlock` which packs data into fixed-size blocks.
struct PackingBlockReader<'a> {
    repo_state: &'a RepoState,
    store_state: &'a mut StoreState,
    pack_size: u32,
}

impl ReadBlock for PackingBlockReader {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let index_list = match self.repo_state.packs.get(&id) {
            Some(pack_index) => pack_index,
            None => return Err(crate::Error::InvalidData),
        };

        let block_size = index_list.iter().map(|index| index.size).sum();
        let mut block_buffer = Vec::with_capacity(block_size as usize);

        // A block can be spread across multiple packs. Get the data from each pack and concatenate
        // them.
        for pack_index in index_list {
            // Check if the data we need is already in the read buffer.
            let pack_buffer = match &self.store_state.read_buffer {
                // Read the data from the read buffer.
                Some(pack) if pack.id == pack_index.id => &pack.buffer,

                // Read a new pack into the read buffer.
                _ => {
                    let encoded_pack_buffer = self
                        .repo_state
                        .store
                        .lock()
                        .unwrap()
                        .read_block(pack_index.id)?
                        .map_err(|error| crate::Store::Error(error))?
                        .ok_or(crate::Error::InvalidData)?;
                    let pack_buffer = self
                        .repo_state
                        .decode_data(encoded_pack_buffer.as_slice())?;
                    let pack = Pack {
                        id: pack_index.id,
                        buffer: pack_buffer,
                    };
                    self.store_state.read_buffer = Some(pack);
                    self.store_state.read_buffer.unwrap().buffer
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

/// A `ReadBlock` and `WriteBlock` which packs data into fixed-size blocks.
struct PackingBlockWriter<'a> {
    repo_state: &'a mut RepoState,
    store_state: &'a mut StoreState,
    pack_size: u32,
}

impl ReadBlock for PackingBlockWriter {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let mut reader = PackingBlockReader {
            repo_state: self.repo_state,
            store_state: self.store_state,
            pack_size: self.pack_size,
        };
        reader.read_block(id)
    }
}

impl WriteBlock for PackingBlockWriter {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()> {
        let current_pack = self
            .store_state
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
            self.repo_state
                .packs
                .entry(id)
                .or_insert_with(Vec::new)
                .push(pack_index);

            // If we've filled the current pack, write it to the data store.
            if current_pack.buffer.len() == self.pack_size {
                let encoded_pack = self
                    .repo_state
                    .encode_data(current_pack.buffer.as_slice())?;
                self.repo_state
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
struct DirectBlockWriter<'a> {
    state: &'a RepoState,
}

impl<'a> DirectBlockWriter<'a> {
    pub fn new(state: &'a RepoState) -> Self {
        DirectBlockWriter { state }
    }
}

impl ReadBlock for DirectBlockWriter {
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

impl WriteBlock for DirectBlockWriter {
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

/// The state for a `StoreReader` or `StoreWriter`.
struct StoreState {
    /// The pack which was most recently read from the data store.
    read_buffer: Option<Pack>,

    /// The pack which is currently being written to.
    write_buffer: Option<Pack>,
}

impl Default for StoreState {
    fn default() -> Self {
        StoreState {
            read_buffer: None,
            write_buffer: None,
        }
    }
}

/// An borrowed type for reading from a data store.
pub struct StoreReader<'a> {
    repo_state: &'a RepoState,
    store_state: StoreState,
}

impl<'a> StoreReader<'a> {
    /// Return a new `StoreReader` which encapsulates the given `state`.
    pub fn new(state: &'a RepoState) -> Self {
        StoreReader {
            repo_state: state,
            store_state: StoreState::default(),
        }
    }
}

impl<'a> ReadBlock for StoreReader<'a> {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let mut reader = match self.repo_state.metadata.packing {
            Packing::None => DirectBlockWriter {
                state: &self.repo_state,
            },
            Packing::Fixed(pack_size) => PackingBlockReader {
                repo_state: &self.repo_state,
                store_state: &mut self.store_state,
                pack_size,
            },
        };
        reader.read_block(id)
    }
}

/// An borrowed type for reading from and writing to a data store.
pub struct StoreWriter<'a> {
    repo_state: &'a mut RepoState,
    store_state: StoreState,
}

impl<'a> StoreWriter<'a> {
    /// Return a new `StoreWriter` which encapsulates the given `state`.
    pub fn new(state: &'a mut RepoState) -> Self {
        StoreWriter {
            repo_state: state,
            store_state: StoreState::default(),
        }
    }
}

impl<'a> ReadBlock for StoreWriter<'a> {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let mut reader = match self.repo_state.metadata.packing {
            Packing::None => DirectBlockWriter {
                state: &self.repo_state,
            },
            Packing::Fixed(pack_size) => PackingBlockReader {
                repo_state: &self.repo_state,
                store_state: &mut self.store_state,
                pack_size,
            },
        };
        reader.read_block(id)
    }
}

impl<'a> WriteBlock for StoreWriter<'a> {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()> {
        let mut writer = match self.repo_state.metadata.packing {
            Packing::None => DirectBlockWriter {
                state: &self.repo_state,
            },
            Packing::Fixed(pack_size) => PackingBlockWriter {
                repo_state: &mut self.repo_state,
                store_state: &mut self.store_state,
                pack_size,
            },
        };
        writer.write_block(id, data)
    }
}

/// Read chunks of data.
pub trait ReadChunk {
    /// Return the bytes of the chunk with the given checksum.
    fn read_chunk(&mut self, chunk: Chunk) -> crate::Result<Vec<u8>>;
}

/// Write chunks of data.
pub trait WriteChunk {
    /// Write the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum may be returned without
    /// writing any new data.
    ///
    /// This requires a unique `id` which is used for reference counting.
    fn write_chunk(&mut self, data: &[u8], id: UniqueId) -> crate::Result<Chunk>;
}

impl ReadChunk for StoreReader {
    fn read_chunk(&mut self, chunk: Chunk) -> crate::Result<Vec<u8>> {
        let chunk_info = self
            .repo_state
            .chunks
            .get(&chunk)
            .ok_or(crate::Error::InvalidData)?;
        self.read_block(chunk_info.block_id)
    }
}

impl WriteChunk for StoreWriter {
    fn write_chunk(&mut self, data: &[u8], id: UniqueId) -> crate::Result<Chunk> {
        // Get a checksum of the unencoded data.
        let chunk = Chunk {
            hash: chunk_hash(data),
            size: data.len(),
        };

        // Check if the chunk already exists.
        if let Some(chunk_info) = self.repo_state.chunks.get_mut(&chunk) {
            chunk_info.references.insert(id);
            return Ok(chunk);
        }

        let block_id = Uuid::new_v4();
        self.write_block(block_id, data)?;

        // Add the chunk to the header.
        let chunk_info = ChunkInfo {
            block_id,
            references: {
                let mut id_set = HashSet::new();
                id_set.insert(id);
                id_set
            },
        };
        self.repo_state.chunks.insert(chunk, chunk_info);

        Ok(chunk)
    }
}
