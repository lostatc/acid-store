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

use std::collections::HashMap;
use std::hash::Hash;
use std::io;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::Compression;
use crate::Encryption;
use crate::HashAlgorithm;
use crate::Object;

/// A persistent storage backend for a repository.
pub trait DataStore: Sized {
    /// A value which can be used to locate a chunk.
    type ChunkId;

    /// Open the data store.
    fn open(&self) -> io::Result<Self>;

    /// Write the given `data` as a new chunk and return its ID.
    ///
    /// Once this method returns `Ok`, the `data` is persistently stored. Until the chunk is freed
    /// with `free_chunk`, calling `read_chunk` with the returned chunk ID will return the `data`
    /// which was written.
    ///
    /// If this method returns `Err` or panics, it is up to the implementation to ensure that any
    /// space allocated to store the given `data` is freed.
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<Self::ChunkId>;

    /// Return the bytes of the chunk with the given `id`.
    ///
    /// If there is no chunk with the given `id` or the chunk has been freed with `free_chunk`, the
    /// contents of the returned buffer is undefined.
    fn read_chunk(&self, id: Self::ChunkId) -> io::Result<Vec<u8>>;

    /// Free the space used by the chunk with the given `id`.
    ///
    /// This is called to mark that a chunk is no longer being referenced and can be deleted or
    /// overwritten with new data by the implementation.
    fn free_chunk(&mut self, id: Self::ChunkId) -> io::Result<()>;

    /// Write the given `metadata` to the repository, overwriting the existing metadata.
    ///
    /// Writing the metadata must be an atomic and consistent operation.
    fn write_metadata(&mut self, metadata: &[u8]) -> io::Result<()>;

    /// Return the metadata for this repository.
    fn read_metadata(&self) -> io::Result<Vec<u8>>;
}
