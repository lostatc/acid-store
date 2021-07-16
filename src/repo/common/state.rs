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

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::Mutex;

use cdchunking::ChunkerImpl;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::store::DataStore;

use super::chunk_store::StoreState;
use super::chunking::IncrementalChunker;
use super::encryption::EncryptionKey;
use super::handle::{Chunk, Extent, ObjectHandle};
use super::id_table::UniqueId;
use super::lock::Lock;
use super::lock::LockTable;
use super::metadata::RepoMetadata;

/// Information about a chunk in a repository.
#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct ChunkInfo {
    /// The ID of the block in the data store which stores this chunk.
    pub block_id: Uuid,

    /// The IDs of objects which reference this chunk.
    pub references: HashSet<UniqueId>,
}

/// The location of a block in a pack.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackIndex {
    /// The UUID of the pack in the data store.
    pub id: Uuid,

    /// The offset from the start of the pack where the block is located.
    pub offset: u32,

    /// The size of the block in bytes.
    pub size: u32,
}

/// A pack which stores multiple blocks.
#[derive(Debug)]
pub struct Pack {
    /// The UUID of this pack in the data store.
    pub id: Uuid,

    /// The data contained in the pack.
    pub buffer: Vec<u8>,
}

impl Pack {
    /// Create a new empty pack with the given `pack_size`.
    pub fn new(pack_size: u32) -> Self {
        Pack {
            id: Uuid::new_v4(),
            buffer: Vec::with_capacity(pack_size as usize),
        }
    }

    /// Return a clone of this buffer padded to `pack_size` with zeroes.
    pub fn padded(&mut self, pack_size: u32) -> Vec<u8> {
        assert!(
            self.buffer.len() <= pack_size as usize,
            "The size of the current pack has exceeded the configured pack size.",
        );
        let mut padded = self.buffer.clone();
        padded.resize(pack_size as usize, 0u8);
        padded
    }
}

/// Information about an instance of a repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// The version ID for the repository type stored in this instance.
    ///
    /// This corresponds to the `OpenRepo::VERSION_ID` of the repository which was created in this
    /// instance.
    pub version_id: Uuid,

    /// The object handle used to store the serialized object map.
    ///
    /// This object handle contains a serialized map of object IDs to object handles for that
    /// instance.
    pub objects: ObjectHandle,
}

/// The state associated with a `KeyRepo`.
#[derive(Debug)]
pub struct RepoState {
    /// The data store which backs this repository.
    pub store: Mutex<Box<dyn DataStore>>,

    /// The metadata for the repository.
    pub metadata: RepoMetadata,

    /// A map of chunk hashes to information about them.
    pub chunks: HashMap<Chunk, ChunkInfo>,

    /// A map of block IDs to their locations in packs.
    pub packs: HashMap<Uuid, Vec<PackIndex>>,

    /// A table used to track current transactions for each object.
    pub transactions: LockTable<UniqueId>,

    /// The master encryption key for the repository.
    pub master_key: EncryptionKey,

    /// The lock on the repository.
    pub lock: Lock<Uuid>,
}

/// A seek position in an object.
pub enum SeekPosition {
    /// The object is empty.
    Empty,

    /// The seek position is at the end of the object.
    End,

    /// The seek position is at the given extent.
    Extent(ExtentLocation),
}

/// The location of an extent in a stream of bytes.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ExtentLocation {
    /// The extent itself.
    pub extent: Extent,

    /// The offset of the start of the chunk from the beginning of the object.
    pub start: u64,

    /// The offset of the seek position from the beginning of the object.
    pub position: u64,

    /// The index of the chunk in the list of chunks.
    pub index: usize,
}

impl ExtentLocation {
    /// The offset of the seek position from the beginning of the extent.
    pub fn relative_position(&self) -> u64 {
        self.position - self.start
    }
}

/// The state associated with an `Object`.
pub struct ObjectState {
    /// An object responsible for buffering and chunking data which has been written.
    pub chunker: IncrementalChunker,

    /// The list of chunks which have been written in the current transaction.
    pub new_chunks: Vec<Chunk>,

    /// The seek position when the transaction was started.
    pub start_position: SeekPosition,

    /// The current seek position of the object.
    pub position: u64,

    /// The chunk which was most recently read from.
    ///
    /// If no data has been read, this is `None`.
    pub buffered_chunk: Option<Chunk>,

    /// The contents of the chunk which was most recently read from.
    pub read_buffer: Vec<u8>,

    /// A pre-allocated buffer of null bytes to read from when reading a hole.
    pub hole_buffer: Vec<u8>,

    /// A lock representing the current transaction if there is one.
    pub transaction_lock: Option<Lock<UniqueId>>,

    /// The state for reading and writing blocks to the data store.
    pub store_state: StoreState,
}

impl ObjectState {
    /// Create a new empty state for a repository with a given chunk size.
    pub fn new(chunker: Box<dyn ChunkerImpl>) -> Self {
        Self {
            chunker: IncrementalChunker::new(chunker),
            new_chunks: Vec::new(),
            start_position: SeekPosition::Empty,
            position: 0,
            buffered_chunk: None,
            read_buffer: Vec::new(),
            hole_buffer: Vec::new(),
            transaction_lock: None,
            store_state: StoreState::new(),
        }
    }
}

impl Debug for ObjectState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectState")
    }
}
