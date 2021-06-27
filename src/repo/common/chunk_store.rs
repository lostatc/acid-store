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

use std::cmp::min;
use std::collections::HashSet;

use uuid::Uuid;

use super::handle::{chunk_hash, Chunk};
use super::id_table::UniqueId;
use super::packing::Packing;
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
        let compressed_data = self.metadata.config.compression.compress(data)?;

        Ok(self
            .metadata
            .config
            .encryption
            .encrypt(compressed_data.as_slice(), &self.master_key))
    }

    fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let decrypted_data = self
            .metadata
            .config
            .encryption
            .decrypt(data, &self.master_key)?;

        Ok(self
            .metadata
            .config
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
pub trait WriteBlock: ReadBlock {
    /// Write the given `data` as a new block with the given `id`.
    ///
    /// If a block with the given `id` already exists, it is overwritten.
    ///
    /// The data is encoded before it is written.
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()>;
}

struct PackingBlockReader<'a> {
    repo_state: &'a RepoState,
    store_state: &'a mut StoreState,
    pack_size: u32,
}

impl<'a> ReadBlock for PackingBlockReader<'a> {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let index_list = match self.repo_state.packs.get(&id) {
            Some(pack_index) => pack_index,
            None => return Err(crate::Error::InvalidData),
        };

        let block_size: u32 = index_list.iter().map(|index| index.size).sum();
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
                        .read_block(pack_index.id)
                        .map_err(crate::Error::Store)?
                        .ok_or(crate::Error::InvalidData)?;
                    let pack_buffer = self
                        .repo_state
                        .decode_data(encoded_pack_buffer.as_slice())?;
                    let pack = Pack {
                        id: pack_index.id,
                        buffer: pack_buffer,
                    };
                    self.store_state.read_buffer = Some(pack);
                    &self.store_state.read_buffer.as_ref().unwrap().buffer
                }
            };

            // Get the slice of the pack containing the block data.
            let start = pack_index.offset as usize;
            let end = (pack_index.offset + pack_index.size) as usize;
            block_buffer.extend_from_slice(&pack_buffer[start..end]);
        }

        Ok(block_buffer)
    }
}

struct PackingBlockWriter<'a> {
    repo_state: &'a mut RepoState,
    store_state: &'a mut StoreState,
    pack_size: u32,
}

impl<'a> ReadBlock for PackingBlockWriter<'a> {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let mut reader = PackingBlockReader {
            repo_state: self.repo_state,
            store_state: self.store_state,
            pack_size: self.pack_size,
        };
        reader.read_block(id)
    }
}

impl<'a> WriteBlock for PackingBlockWriter<'a> {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()> {
        let pack_size = self.pack_size;
        let current_pack = self
            .store_state
            .write_buffer
            .get_or_insert_with(|| Pack::new(pack_size));

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

        // The list packs which store the current block and where it's located in those packs.
        let mut new_packs_indices = Vec::new();

        loop {
            // Fill the current pack with the provided `data`.
            remaining_space = self.pack_size as usize - current_pack.buffer.len();
            buffer_end = min(bytes_written + remaining_space, data.len());
            next_buffer = &data[bytes_written..buffer_end];
            current_pack.buffer.extend_from_slice(next_buffer);
            bytes_written += next_buffer.len();
            current_size += next_buffer.len() as u32;

            assert!(
                current_pack.buffer.len() <= self.pack_size as usize,
                "The size of the current pack has exceeded the configured pack size."
            );

            assert!(
                bytes_written <= data.len(),
                "More bytes were written than are available in the provided buffer."
            );

            // Add the location of this block in the pack to the list.
            let pack_index = PackIndex {
                id: current_pack.id,
                offset: current_offset,
                size: current_size,
            };
            new_packs_indices.push(pack_index);

            // If we've filled the current pack, write it to the data store.
            if current_pack.buffer.len() == self.pack_size as usize {
                let encoded_pack = self
                    .repo_state
                    .encode_data(current_pack.buffer.as_slice())?;
                self.repo_state
                    .store
                    .lock()
                    .unwrap()
                    .write_block(current_pack.id, encoded_pack.as_slice())
                    .map_err(crate::Error::Store)?;

                // We're starting a new pack, so these need to be reset.
                current_offset = 0;
                current_size = 0;

                *current_pack = Pack::new(self.pack_size);
            }

            // Break once we've written all the `data`.
            if bytes_written == data.len() {
                // Once we've exhausted all the bytes in `data`, we need to pad the currently
                // buffered pack with zeroes and write it to the data store. The contract of this
                // interface guarantees that all data will be written to the data store once it
                // returns, and we won't have the opportunity to flush it later. We'll keep a clone
                // of this pack buffered in memory though, so we can write more data to the pack and
                // overwrite it in the data store in the future. This way, we don't have a bunch of
                // half-empty packs in the data store.
                let padded_pack = current_pack.padded(self.pack_size);
                let encoded_pack = self.repo_state.encode_data(padded_pack.as_slice())?;
                self.repo_state
                    .store
                    .lock()
                    .unwrap()
                    .write_block(current_pack.id, encoded_pack.as_slice())
                    .map_err(crate::Error::Store)?;

                // We need to update the pack map in the repository state after all data has been
                // written to the data store. If this method fails early, we can't have the pack map
                // referencing data which hasn't been written to the data store. If this method
                // fails and there is data in the data store which isn't referenced in the pack map,
                // we'll have the opportunity to clean up the unreferenced data later.
                //
                // The contract of this interface guarantees that if a block with this `id` is
                // already in the data store, it is replaced. We can't remove the unreferenced data
                // from the data store at this point in case the repository is rolled back, but we
                // do need to replace the pack indices in the pack map, which we do here.
                self.repo_state.packs.insert(id, new_packs_indices);

                return Ok(());
            }
        }
    }
}

struct DirectBlockWriter<'a> {
    state: &'a RepoState,
}

impl<'a> DirectBlockWriter<'a> {
    fn new(state: &'a RepoState) -> Self {
        DirectBlockWriter { state }
    }
}

impl<'a> ReadBlock for DirectBlockWriter<'a> {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let encoded_block = self
            .state
            .store
            .lock()
            .unwrap()
            .read_block(id)
            .map_err(crate::Error::Store)?
            .ok_or(crate::Error::InvalidData)?;
        self.state.decode_data(encoded_block.as_slice())
    }
}

impl<'a> WriteBlock for DirectBlockWriter<'a> {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()> {
        let encoded_block = self.state.encode_data(data)?;
        self.state
            .store
            .lock()
            .unwrap()
            .write_block(id, encoded_block.as_slice())
            .map_err(crate::Error::Store)
    }
}

/// The state for a `StoreReader` or `StoreWriter`.
#[derive(Debug)]
pub struct StoreState {
    /// The pack which was most recently read from the data store.
    read_buffer: Option<Pack>,

    /// The pack which is currently being written to.
    write_buffer: Option<Pack>,
}

impl StoreState {
    /// Create a new empty `StoreState`.
    pub fn new() -> Self {
        StoreState {
            read_buffer: None,
            write_buffer: None,
        }
    }
}

/// Read chunks of data.
pub trait ReadChunk {
    /// Return the bytes of the chunk with the given checksum.
    fn read_chunk(&mut self, chunk: Chunk) -> crate::Result<Vec<u8>>;
}

/// Write chunks of data.
pub trait WriteChunk: ReadChunk {
    /// Write the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum may be returned without
    /// writing any new data.
    ///
    /// This requires a unique `id` which is used for reference counting.
    fn write_chunk(&mut self, data: &[u8], id: UniqueId) -> crate::Result<Chunk>;
}

/// A borrowed type for reading from a data store.
pub struct StoreReader<'a> {
    repo_state: &'a RepoState,
    store_state: &'a mut StoreState,
}

impl<'a> StoreReader<'a> {
    /// Create a new instance which borrows the given state.
    pub fn new(repo_state: &'a RepoState, store_state: &'a mut StoreState) -> Self {
        StoreReader {
            repo_state,
            store_state,
        }
    }
}

impl<'a> ReadBlock for StoreReader<'a> {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let mut read_block: Box<dyn ReadBlock> = match &self.repo_state.metadata.config.packing {
            Packing::None => Box::new(DirectBlockWriter {
                state: &self.repo_state,
            }),
            Packing::Fixed(pack_size) => Box::new(PackingBlockReader {
                repo_state: &self.repo_state,
                store_state: &mut self.store_state,
                pack_size: *pack_size,
            }),
        };
        read_block.read_block(id)
    }
}

impl<'a> ReadChunk for StoreReader<'a> {
    fn read_chunk(&mut self, chunk: Chunk) -> crate::Result<Vec<u8>> {
        let chunk_info = self
            .repo_state
            .chunks
            .get(&chunk)
            .ok_or(crate::Error::InvalidData)?;
        self.read_block(chunk_info.block_id)
    }
}

/// A borrowed type for reading from and writing to a data store.
pub struct StoreWriter<'a> {
    repo_state: &'a mut RepoState,
    store_state: &'a mut StoreState,
}

impl<'a> StoreWriter<'a> {
    /// Create a new instance which borrows the given state.
    pub fn new(repo_state: &'a mut RepoState, store_state: &'a mut StoreState) -> Self {
        StoreWriter {
            repo_state,
            store_state,
        }
    }
}

impl<'a> ReadBlock for StoreWriter<'a> {
    fn read_block(&mut self, id: Uuid) -> crate::Result<Vec<u8>> {
        let mut chunk_reader = StoreReader {
            repo_state: self.repo_state,
            store_state: self.store_state,
        };
        chunk_reader.read_block(id)
    }
}

impl<'a> WriteBlock for StoreWriter<'a> {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> crate::Result<()> {
        let mut block_writer: Box<dyn WriteBlock> =
            match self.repo_state.metadata.config.packing.clone() {
                Packing::None => Box::new(DirectBlockWriter {
                    state: &self.repo_state,
                }),
                Packing::Fixed(pack_size) => Box::new(PackingBlockWriter {
                    repo_state: &mut self.repo_state,
                    store_state: &mut self.store_state,
                    pack_size,
                }),
            };
        block_writer.write_block(id, data)
    }
}

impl<'a> ReadChunk for StoreWriter<'a> {
    fn read_chunk(&mut self, chunk: Chunk) -> crate::Result<Vec<u8>> {
        let mut chunk_reader = StoreReader {
            repo_state: self.repo_state,
            store_state: self.store_state,
        };
        chunk_reader.read_chunk(chunk)
    }
}

impl<'a> WriteChunk for StoreWriter<'a> {
    fn write_chunk(&mut self, data: &[u8], id: UniqueId) -> crate::Result<Chunk> {
        assert!(
            data.len() <= std::u32::MAX as usize,
            "Given data exceeds maximum chunk size."
        );

        // Get a checksum of the unencoded data.
        let chunk = Chunk {
            hash: chunk_hash(data),
            size: data.len() as u32,
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
