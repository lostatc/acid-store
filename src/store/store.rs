/*
 * Copyright 2019 Garrett Powell
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

use std::io;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// A persistent store for storing chunks of data.
pub trait ChunkStore {
    /// A value which uniquely identifies a chunk.
    type ChunkId: Eq + Clone + Serialize + DeserializeOwned;

    /// Write the given `data` as a new chunk and return its ID.
    ///
    /// If a chunk with the same ID already exists, it is overwritten.
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<Self::ChunkId>;

    /// Return the bytes of the chunk with the given `id`.
    ///
    /// If there is no chunk with the given id, the contents of the returned buffer is undefined.
    fn read_chunk(&self, id: &Self::ChunkId) -> io::Result<Vec<u8>>;

    /// Remove the chunk with the given `id` from the store if it exists.
    fn remove_chunk(&mut self, id: &Self::ChunkId) -> io::Result<()>;

    /// Return an iterator of IDs of chunks in the store.
    fn list_chunks(&self) -> io::Result<Box<dyn Iterator<Item=io::Result<Self::ChunkId>>>>;
}

/// A persistent store for storing metadata.
pub trait MetadataStore {
    /// Write the given `metadata` to the store, overwriting the existing metadata.
    ///
    /// Writing the metadata must be an atomic operation.
    fn write_metadata(&mut self, metadata: &[u8]) -> io::Result<()>;

    /// Return the metadata in the store.
    fn read_metadata(&self) -> io::Result<Vec<u8>>;
}

/// A storage backend for a repository.
pub trait DataStore: ChunkStore + MetadataStore {}
