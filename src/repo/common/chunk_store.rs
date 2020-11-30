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

use super::block_store::{BlockReader, BlockWriter, DirectBlockStore, PackingBlockStore};
use super::id_table::UniqueId;
use super::object::{chunk_hash, Chunk};
use super::packing::Packing;
use super::state::{ChunkInfo, RepoState};

/// Read chunks of data.
pub trait ChunkReader {
    /// Return the bytes of the chunk with the given checksum.
    fn read_chunk(&mut self, chunk: Chunk) -> crate::Result<Vec<u8>>;
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

impl RepoState {
    /// Return a block store for this repository.
    fn block_store(&mut self) -> Box<dyn BlockReader + BlockWriter> {
        match self.metadata.packing {
            Packing::None => Box::new(DirectBlockStore::new(self)),
            Packing::Fixed(pack_size) => Box::new(PackingBlockStore::new(self, pack_size)),
        }
    }
}

impl ChunkReader for RepoState {
    fn read_chunk(&mut self, chunk: Chunk) -> crate::Result<Vec<u8>> {
        let chunk_info = self.chunks.get(&chunk).ok_or(crate::Error::InvalidData)?;
        self.block_store().read_block(chunk_info.block_id)
    }
}

impl ChunkWriter for RepoState {
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

        let block_id = Uuid::new_v4();
        self.block_store().write_block(block_id, data)?;

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
