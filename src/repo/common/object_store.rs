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

use std::cmp::{min, Ordering};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};

use rmp_serde::{from_read, to_vec};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::chunk_store::{ReadChunk, StoreReader, StoreWriter, WriteChunk};
use super::handle::{chunk_hash, ContentId, Extent, ObjectHandle, ObjectStats};
use super::state::{ExtentLocation, ObjectState, RepoState, SeekPosition};

pub struct ObjectStore {
    repo_state: Arc<RwLock<RepoState>>,
    handle: Arc<RwLock<ObjectHandle>>,
}

impl ObjectStore {
    pub fn new(
        repo_state: &Weak<RwLock<RepoState>>,
        handle: &Weak<RwLock<ObjectHandle>>,
    ) -> crate::Result<Self> {
        Ok(Self {
            repo_state: repo_state.upgrade().ok_or(crate::Error::InvalidObject)?,
            handle: handle.upgrade().ok_or(crate::Error::InvalidObject)?,
        })
    }

    pub fn info_guard<'a>(&'a self, object_state: &'a ObjectState) -> ObjectInfoGuard<'a> {
        ObjectInfoGuard {
            repo_state: self.repo_state.read().unwrap(),
            handle: self.handle.read().unwrap(),
            object_state,
        }
    }

    pub fn reader_guard<'a>(&'a self, object_state: &'a mut ObjectState) -> ObjectReaderGuard<'a> {
        ObjectReaderGuard {
            repo_state: self.repo_state.read().unwrap(),
            handle: self.handle.read().unwrap(),
            object_state,
        }
    }

    pub fn writer_guard<'a>(&'a self, object_state: &'a mut ObjectState) -> ObjectWriterGuard<'a> {
        ObjectWriterGuard {
            repo_state: self.repo_state.write().unwrap(),
            handle: self.handle.write().unwrap(),
            object_state,
        }
    }
}

pub struct ObjectInfoGuard<'a> {
    repo_state: RwLockReadGuard<'a, RepoState>,
    handle: RwLockReadGuard<'a, ObjectHandle>,
    object_state: &'a ObjectState,
}

impl<'a> ObjectInfoGuard<'a> {
    pub fn info(&self) -> ObjectInfo {
        ObjectInfo::new(&self.repo_state, &self.object_state, &self.handle)
    }
}

pub struct ObjectReaderGuard<'a> {
    repo_state: RwLockReadGuard<'a, RepoState>,
    handle: RwLockReadGuard<'a, ObjectHandle>,
    object_state: &'a mut ObjectState,
}

impl<'a> ObjectReaderGuard<'a> {
    pub fn info(&self) -> ObjectInfo {
        ObjectInfo::new(&self.repo_state, &self.object_state, &self.handle)
    }

    pub fn reader(&mut self) -> ObjectReader {
        ObjectReader::new(&self.repo_state, &mut self.object_state, &self.handle)
    }
}

pub struct ObjectWriterGuard<'a> {
    repo_state: RwLockWriteGuard<'a, RepoState>,
    handle: RwLockWriteGuard<'a, ObjectHandle>,
    object_state: &'a mut ObjectState,
}

impl<'a> ObjectWriterGuard<'a> {
    pub fn info(&self) -> ObjectInfo {
        ObjectInfo::new(&self.repo_state, &self.object_state, &self.handle)
    }

    pub fn reader(&mut self) -> ObjectReader {
        ObjectReader::new(&self.repo_state, &mut self.object_state, &self.handle)
    }

    pub fn writer(&mut self) -> ObjectWriter {
        ObjectWriter::new(
            &mut self.repo_state,
            &mut self.object_state,
            &mut self.handle,
        )
    }
}

/// A borrowed value for getting information about an object.
pub struct ObjectInfo<'a> {
    repo_state: &'a RepoState,
    object_state: &'a ObjectState,
    handle: &'a ObjectHandle,
}

impl<'a> ObjectInfo<'a> {
    pub fn new(
        repo_state: &'a RepoState,
        object_state: &'a ObjectState,
        handle: &'a ObjectHandle,
    ) -> Self {
        Self {
            repo_state,
            object_state,
            handle,
        }
    }

    /// Return the size of the object in bytes.
    pub fn size(&self) -> crate::Result<u64> {
        if self.object_state.transaction_lock.is_some() {
            return Err(crate::Error::TransactionInProgress);
        }
        Ok(self.handle.size())
    }

    /// Return a `ContentId` representing the contents of the object.
    pub fn content_id(&self) -> crate::Result<ContentId> {
        if self.object_state.transaction_lock.is_some() {
            return Err(crate::Error::TransactionInProgress);
        }
        Ok(ContentId {
            repo_id: self.repo_state.metadata.id,
            extents: self.handle.extents.clone(),
        })
    }

    /// Return an `ObjectStats` containing statistics about the object.
    pub fn stats(&self) -> crate::Result<ObjectStats> {
        if self.object_state.transaction_lock.is_some() {
            return Err(crate::Error::TransactionInProgress);
        }
        let mut current_position = 0u64;
        let mut actual_size = 0u64;
        let mut apparent_size = 0u64;
        let mut holes = Vec::new();

        for extent in &self.handle.extents {
            match extent {
                Extent::Chunk(_) => {
                    actual_size += extent.size();
                }
                Extent::Hole { .. } => {
                    holes.push(current_position..(current_position + extent.size()));
                }
            }
            current_position += extent.size();
            apparent_size += extent.size();
        }

        Ok(ObjectStats {
            actual: actual_size,
            apparent: apparent_size,
            holes,
        })
    }
}

/// A borrowed value for reading from an object.
pub struct ObjectReader<'a> {
    repo_state: &'a RepoState,
    object_state: &'a mut ObjectState,
    handle: &'a ObjectHandle,
}

/// A wrapper for reading data from an object.
impl<'a> ObjectReader<'a> {
    pub fn new(
        repo_state: &'a RepoState,
        object_state: &'a mut ObjectState,
        handle: &'a ObjectHandle,
    ) -> Self {
        Self {
            repo_state,
            object_state,
            handle,
        }
    }

    fn store_reader(&mut self) -> StoreReader {
        StoreReader::new(self.repo_state, &mut self.object_state.store_state)
    }

    /// Verify the integrity of the data in this object.
    pub fn verify(&mut self) -> crate::Result<bool> {
        if self.object_state.transaction_lock.is_some() {
            return Err(crate::Error::TransactionInProgress);
        }

        let expected_chunks = self.handle.chunks().collect::<Vec<_>>();

        for chunk in expected_chunks {
            match self.store_reader().read_chunk(chunk) {
                Ok(data) => {
                    if data.len() != chunk.size as usize || chunk_hash(&data) != chunk.hash {
                        return Ok(false);
                    }
                }
                // Ciphertext verification failed. No need to check the hash.
                Err(crate::Error::InvalidData) => return Ok(false),
                Err(error) => return Err(error),
            }
        }

        Ok(true)
    }

    /// Return the current seek position in the object.
    fn current_position(&self) -> SeekPosition {
        if self.handle.extents.is_empty() {
            return SeekPosition::Empty;
        }

        let mut extent_start = 0u64;
        let mut extent_end = 0u64;

        for (index, extent) in self.handle.extents.iter().enumerate() {
            extent_end += extent.size();
            if self.object_state.position >= extent_start && self.object_state.position < extent_end
            {
                return SeekPosition::Extent(ExtentLocation {
                    extent: *extent,
                    start: extent_start,
                    position: self.object_state.position,
                    index,
                });
            }
            extent_start += extent.size();
        }

        SeekPosition::End
    }

    /// Return a slice of null bytes of the given `size`.
    fn read_hole(&mut self, size: usize) -> &[u8] {
        if self.object_state.hole_buffer.len() < size {
            self.object_state.hole_buffer.resize(size, 0);
        }
        &self.object_state.hole_buffer[..size]
    }

    /// Return the slice of bytes between the current seek position and the end of the extent.
    ///
    /// The returned slice will be no longer than `size`.
    fn read_extent(&mut self, size: usize) -> crate::Result<&[u8]> {
        // If the object is empty or we're at the end of the object, there's no data to read.
        let current_location = match self.current_position() {
            SeekPosition::Empty | SeekPosition::End => return Ok(&[]),
            SeekPosition::Extent(location) => location,
        };

        match current_location.extent {
            Extent::Chunk(chunk) => {
                // If we're reading from a new chunk, read the contents of that chunk into the read
                // buffer.
                if Some(chunk) != self.object_state.buffered_chunk {
                    self.object_state.buffered_chunk = Some(chunk);
                    self.object_state.read_buffer = self.store_reader().read_chunk(chunk)?;
                }

                let start = current_location.relative_position() as usize;
                let end = min(start + size, chunk.size as usize);
                Ok(&self.object_state.read_buffer[start..end])
            }
            Extent::Hole { size: hole_size } => {
                let read_size = min(
                    size as u64,
                    hole_size - current_location.relative_position(),
                ) as usize;
                Ok(self.read_hole(read_size))
            }
        }
    }

    /// Deserialize a value serialized with `ObjectWriter::serialize`.
    pub fn deserialize<T: DeserializeOwned>(&mut self) -> crate::Result<T> {
        self.seek(SeekFrom::Start(0))?;
        from_read(self).map_err(|_| crate::Error::Deserialize)
    }
}

impl<'a> Seek for ObjectReader<'a> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        if self.object_state.transaction_lock.is_some() {
            return Err(crate::Error::TransactionInProgress.into());
        }

        let object_size = self.handle.size();

        let new_position = match pos {
            SeekFrom::Start(offset) => min(object_size, offset),
            SeekFrom::End(offset) => {
                if offset > object_size as i64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Attempted to seek to a negative offset.",
                    ));
                } else {
                    min(object_size, (object_size as i64 - offset) as u64)
                }
            }
            SeekFrom::Current(offset) => {
                if self.object_state.position as i64 + offset < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Attempted to seek to a negative offset.",
                    ));
                } else {
                    min(
                        object_size,
                        (self.object_state.position as i64 + offset) as u64,
                    )
                }
            }
        };

        self.object_state.position = new_position;
        Ok(new_position)
    }
}

// To avoid reading the same chunk from the repository multiple times, the chunk which was most
// recently read from is cached in a buffer.
impl<'a> Read for ObjectReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.object_state.transaction_lock.is_some() {
            return Err(crate::Error::TransactionInProgress.into());
        }

        let next_chunk = self.read_extent(buf.len())?;
        let bytes_read = next_chunk.len();
        buf[..bytes_read].copy_from_slice(next_chunk);
        self.object_state.position += bytes_read as u64;
        Ok(bytes_read)
    }
}

/// A borrowed value for writing to an object.
pub struct ObjectWriter<'a> {
    repo_state: &'a mut RepoState,
    object_state: &'a mut ObjectState,
    handle: &'a mut ObjectHandle,
}

impl<'a> ObjectWriter<'a> {
    pub fn new(
        repo_state: &'a mut RepoState,
        object_state: &'a mut ObjectState,
        handle: &'a mut ObjectHandle,
    ) -> Self {
        Self {
            repo_state,
            object_state,
            handle,
        }
    }

    fn store_writer(&mut self) -> StoreWriter {
        StoreWriter::new(&mut self.repo_state, &mut self.object_state.store_state)
    }

    fn object_reader(&mut self) -> ObjectReader {
        ObjectReader {
            repo_state: self.repo_state,
            object_state: self.object_state,
            handle: self.handle,
        }
    }

    /// Truncate the object to the given `length`.
    fn truncate(&mut self, size: u64) -> crate::Result<()> {
        if size >= self.handle.size() {
            return Ok(());
        }

        let original_position = self.object_state.position;
        self.object_state.position = size;

        let end_location = match self.object_reader().current_position() {
            SeekPosition::Extent(location) => location,
            SeekPosition::Empty | SeekPosition::End => return Ok(()),
        };
        let new_last_extent = match end_location.extent {
            // Truncating the object may mean slicing a chunk in half. Because we can't edit chunks
            // in-place, we need to read the final chunk, slice it, and write it back.
            Extent::Chunk(chunk) => {
                let last_chunk_data = self.store_writer().read_chunk(chunk)?;
                let new_last_chunk_data =
                    &last_chunk_data[..end_location.relative_position() as usize];
                let handle_id = self.handle.id;
                Extent::Chunk(
                    self.store_writer()
                        .write_chunk(&new_last_chunk_data, handle_id)?,
                )
            }
            Extent::Hole { .. } => Extent::Hole {
                size: end_location.relative_position(),
            },
        };

        // Remove all extents including and after the final chunk.
        self.handle.extents.drain(end_location.index..);

        // Append the new final extent which has been sliced.
        self.handle.extents.push(new_last_extent);

        // Restore the seek position.
        self.object_state.position = min(original_position, size);

        // Release the current transaction.
        self.object_state.transaction_lock = None;

        Ok(())
    }

    /// Extend the object to the given `length`.
    fn extend(&mut self, size: u64) {
        if size <= self.handle.size() {
            return;
        }

        let hole = Extent::Hole {
            size: size - self.handle.size(),
        };
        self.handle.extents.push(hole);
    }

    /// Set the length of the object.
    pub fn set_len(&mut self, size: u64) -> crate::Result<()> {
        // Because this modifies the object, we need to start a new transaction.
        match self.object_state.transaction_lock {
            None => match self.repo_state.transactions.acquire_lock(self.handle.id) {
                None => return Err(crate::Error::TransactionInProgress),
                Some(lock) => {
                    self.object_state.transaction_lock = Some(lock);
                }
            },
            Some(_) => return Err(crate::Error::TransactionInProgress),
        }

        match size.cmp(&self.handle.size()) {
            Ordering::Less => self.truncate(size)?,
            Ordering::Greater => self.extend(size),
            _ => {}
        }

        self.object_state.transaction_lock = None;

        Ok(())
    }

    /// Write chunks stored in the chunker to the repository.
    fn write_chunks(&mut self) -> crate::Result<()> {
        for chunk_data in self.object_state.chunker.chunks() {
            let handle_id = self.handle.id;
            let chunk = self.store_writer().write_chunk(&chunk_data, handle_id)?;
            self.object_state.new_chunks.push(chunk);
        }
        Ok(())
    }

    /// Serialize the given `value` and write it to the object.
    pub fn serialize<T: Serialize>(&mut self, value: &T) -> crate::Result<()> {
        let serialized = to_vec(value).map_err(|_| crate::Error::Serialize)?;
        self.seek(SeekFrom::Start(0))?;
        self.write_all(serialized.as_slice())?;
        self.commit()?;
        self.set_len(serialized.len() as u64)?;
        Ok(())
    }

    /// Commit change to the data store.
    pub fn commit(&mut self) -> crate::Result<()> {
        if self.object_state.transaction_lock.is_none() {
            // No new data has been written since data was last committed.
            return Ok(());
        }

        let current_position = self.object_reader().current_position();

        // If the start position was in a hole, we will need to prepend a new hole when we replace
        // that extent in the object handle. Get the size of the hole we should prepend.
        let start_hole_size = match &self.object_state.start_position {
            SeekPosition::Empty | SeekPosition::End => None,
            SeekPosition::Extent(location) => match location.extent {
                Extent::Chunk(_) => None,
                Extent::Hole { .. } => Some(location.relative_position()),
            },
        };

        // If the current position is in a hole, we will need to append a new hole when we replace
        // that extent in the object handle. Get the size of the hole we should append.
        let end_hole_size = match &current_position {
            SeekPosition::Empty | SeekPosition::End => None,
            SeekPosition::Extent(location) => match location.extent {
                Extent::Chunk(_) => None,
                Extent::Hole { size } => Some(size - location.relative_position()),
            },
        };

        // If the current seek position is in a chunk, we need to make sure the data after
        // the seek position is saved when we replace the current extent. Read this data
        // from the repository and write it to the chunker.
        if let SeekPosition::Extent(location) = &current_position {
            if let Extent::Chunk(chunk) = location.extent {
                let last_chunk = self.store_writer().read_chunk(chunk)?;
                self.object_state
                    .chunker
                    .write_all(&last_chunk[location.relative_position() as usize..])?;
            }
        }

        // Write all the remaining data in the chunker to the repository.
        self.object_state.chunker.flush()?;
        self.write_chunks()?;

        // Find the index of the first extent which is being overwritten.
        let start_index = match &self.object_state.start_position {
            SeekPosition::Empty => 0,
            SeekPosition::End => self.handle.extents.len(),
            SeekPosition::Extent(location) => location.index,
        };

        // Find the index of the last extent which is being overwritten.
        let end_index = match &current_position {
            SeekPosition::Empty => 0,
            SeekPosition::End => self.handle.extents.len(),
            SeekPosition::Extent(location) => location.index + 1,
        };

        // Get the list of new extents we should splice into the object, including any necessary
        // holes at the start or end.
        let mut new_extents = Vec::new();
        if let Some(hole_size) = start_hole_size {
            new_extents.push(Extent::Hole { size: hole_size });
        }
        new_extents.extend(
            self.object_state
                .new_chunks
                .drain(..)
                .map(|chunk| Extent::Chunk(chunk)),
        );
        if let Some(hole_size) = end_hole_size {
            new_extents.push(Extent::Hole { size: hole_size });
        }

        // Update extent references in the object handle to reflect changes.
        self.handle
            .extents
            .splice(start_index..end_index, new_extents);

        // Release the current transaction.
        self.object_state.transaction_lock = None;

        Ok(())
    }
}

// Content-defined chunking makes writing and seeking more complicated. Chunks can't be modified
// in-place; they can only be read or written in their entirety. This means we need to do a lot of
// buffering to wait for a chunk boundary before writing a chunk to the repository. It also means
// the user needs to explicitly call `commit` when they're done writing data.
impl<'a> Write for ObjectWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Attempt to acquire a transaction lock if one has not already been acquired.
        let first_write = match self.object_state.transaction_lock {
            None => match self.repo_state.transactions.acquire_lock(self.handle.id) {
                None => return Err(crate::Error::TransactionInProgress.into()),
                Some(lock) => {
                    self.object_state.transaction_lock = Some(lock);
                    true
                }
            },
            Some(_) => false,
        };

        // Check if this is a new transaction.
        if first_write {
            // Because we're starting a new transaction, we need to set the starting position.
            self.object_state.start_position = self.object_reader().current_position();

            // If the current extent is a chunk, we need to make sure the data before the seek
            // position is saved when we replace the extent on commit. Read this data from the
            // repository and write it to the chunker.
            if let SeekPosition::Extent(location) = &self.object_state.start_position {
                if let Extent::Chunk(chunk) = location.extent {
                    let position = location.relative_position() as usize;
                    let chunk_data = self.store_writer().read_chunk(chunk)?;
                    self.object_state
                        .chunker
                        .write_all(&chunk_data[..position])?;
                }
            }
        }

        // Chunk the data and write any complete chunks to the repository.
        self.object_state.chunker.write_all(buf)?;
        self.write_chunks()?;

        // Advance the seek position.
        self.object_state.position += buf.len() as u64;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Seek for ObjectWriter<'a> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.object_reader().seek(pos)
    }
}

impl<'a> Read for ObjectWriter<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.object_reader().read(buf)
    }
}
