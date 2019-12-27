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

use std::cmp::min;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::iter;
use std::mem::size_of;
use std::path::Path;

use num_integer::div_ceil;
use num_integer::div_floor;
use rmp_serde::{from_read, to_vec};

use crate::store::DataStore;

use super::block::{Chunk, Extent, pad_to_block_size, SuperBlock};
use super::config::FileDataStoreConfig;

pub struct FileDataStore {
    file: File,
    superblock: SuperBlock,
}

impl FileDataStore {
    /// Create a new `FileDataStore` at the given `file` path.
    pub fn create(file: &Path, config: FileDataStoreConfig) -> io::Result<Self> {
        unimplemented!()
    }

    /// Returns the data in the given `extent`.
    fn read_extent(&self, extent: Extent) -> io::Result<Vec<u8>> {
        let mut archive_file = &self.file;

        archive_file.seek(SeekFrom::Start(extent.start(self.superblock.block_size)))?;

        let mut buffer = Vec::with_capacity(extent.length(self.superblock.block_size) as usize);

        self.archive_file
            .try_clone()?
            .take(extent.length(self.superblock.block_size))
            .read_to_end(&mut buffer)?;

        Ok(buffer)
    }

    /// Writes the given `data` to the given `extent` and returns the number of bytes written.
    fn write_extent(&mut self, extent: Extent, data: &[u8]) -> io::Result<usize> {
        self.file.seek(SeekFrom::Start(extent.start(self.superblock.block_size)))?;

        let bytes_written = min(
            data.len(),
            extent.length(self.superblock.block_size) as usize,
        );

        self.archive_file.write_all(&data[..bytes_written])?;

        Ok(bytes_written)
    }

    /// Mark the given `extent` as unused so that it can be overwritten with new data.
    fn free_extent(&mut self, extent: Extent) -> io::Result<()> {}

    /// Returns a list of extents which are unused and can be overwritten.
    ///
    /// The returned extents are sorted by their location in the file.
    fn unused_extents(&mut self) -> io::Result<Vec<Extent>> {
        // Get all extents which are currently part of a chunk.
        let mut used_extents = self.superblock.used_extents.to_vec();

        // Sort extents by their location in the file.
        used_extents.sort_by_key(|extent| extent.index);

        // Get the extents which are unused.
        let initial_extent = Extent {
            index: 0,
            blocks: 0,
        };
        let unused_extents = iter::once(initial_extent)
            .chain(used_extents)
            .collect::<Vec<_>>()
            .windows(2)
            .filter_map(|pair| pair[0].between(pair[1]))
            .collect::<Vec<_>>();

        Ok(unused_extents)
    }

    /// Allocate a new extent of at least `size` bytes at the end of the archive.
    ///
    /// This pads the file so that the new extent is aligned with the block size.
    fn new_extent(&mut self, size: u64) -> io::Result<Extent> {
        let offset = pad_to_block_size(&mut self.file, self.superblock.block_size)?;
        let index = div_floor(offset, self.superblock.block_size as u64);
        let blocks = div_ceil(size, self.superblock.block_size as u64);
        Ok(Extent { index, blocks })
    }

    /// Truncate the given extent so it is just large enough to hold `size` bytes.
    fn truncate_extent(&self, extent: Extent, size: u64) -> Extent {
        let blocks = min(
            extent.blocks,
            div_ceil(size, self.superblock.block_size as u64),
        );
        Extent {
            index: extent.index,
            blocks,
        }
    }

    /// Allocate a contiguous extent of at least `size` bytes.
    fn allocate_contiguous_extent(&mut self, size: u64) -> io::Result<Extent> {
        // Look for an unused extent that is large enough.
        let unused_extents = self.unused_extents()?;
        let allocated_extent = unused_extents
            .iter()
            .find(|extent| extent.length(self.superblock.block_size) >= size);

        // If there is no unused extent large enough, allocate a new extent at the end of the file.
        let allocated_extent = match allocated_extent {
            Some(extent) => self.truncate_extent(*extent, size),
            None => self.new_extent(size)?,
        };

        Ok(allocated_extent)
    }

    /// Allocate a list of extents with a combined size of at least `size` bytes.
    fn allocate_extents(&mut self, size: u64) -> io::Result<Vec<Extent>> {
        let mut allocated_extents = Vec::new();
        let mut bytes_remaining = size;

        // Try to fill all unused extents first.
        for extent in self.unused_extents()? {
            if bytes_remaining <= 0 {
                break;
            }

            let new_extent = self.truncate_extent(extent, bytes_remaining);
            allocated_extents.push(new_extent);
            bytes_remaining -= min(
                bytes_remaining,
                new_extent.length(self.superblock.block_size),
            );
        }

        // If there's still data left, allocate an extent at the end of the file to store the rest.
        if bytes_remaining > 0 {
            allocated_extents.push(self.new_extent(bytes_remaining)?);
        }

        Ok(allocated_extents)
    }

    fn read_superblock(mut file: &File) -> io::Result<SuperBlock> {
        // Read the position and size of the superblock.
        let mut position_buffer = [0u8; size_of::<u64>()];
        let mut size_buffer = [0u8; size_of::<u32>()];
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut position_buffer)?;
        file.read_exact(&mut size_buffer)?;
        let position = u64::from_be_bytes(position_buffer);
        let size = u32::from_be_bytes(size_buffer);

        // Read the bytes of the superblock.
        let mut superblock_buffer = vec![0u8; size as usize];
        file.seek(SeekFrom::Start(position))?;
        file.read_exact(&mut superblock_buffer)?;

        // Deserialize the superblock.
        from_read(superblock_buffer.as_slice()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "The superblock is corrupt. This is most likely unrecoverable.",
            )
        })
    }

    fn write_superblock(&mut self) -> io::Result<()> {
        // Serialize the superblock.
        let serialized_superblock =
            to_vec(&self.superblock).expect("Could not serialize the superblock.");

        // Allocate space for the new superblock.
        let superblock_extent =
            self.allocate_contiguous_extent(serialized_superblock.len() as u64)?;

        // Write the new superblock to the allocated space.
        self.write_extent(superblock_extent, serialized_superblock.as_slice())?;

        let position = superblock_extent.start(self.superblock.block_size);
        let size = serialized_superblock.len() as u32;
        let mut superblock_address = Vec::new();
        superblock_address.extend_from_slice(&position.to_be_bytes());
        superblock_address.extend_from_slice(&size.to_be_bytes());

        // Write the position and size of the superblock.
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&superblock_address)?;

        Ok(())
    }
}

impl DataStore for FileDataStore {
    type ChunkId = Chunk;

    fn open(&self) -> io::Result<Self> {
        unimplemented!()
    }

    fn write_chunk(&mut self, data: &[u8]) -> io::Result<Self::ChunkId> {
        // Get the list of extents which will hold the data.
        let extents = self.allocate_extents(data.len() as u64)?;

        // Write the encoded data to the extents.
        let mut bytes_written = 0;
        for extent in &extents {
            bytes_written += self.write_extent(*extent, &encoded_data[bytes_written..])?;
        }

        // Add this chunk to the header.
        let chunk_id = Chunk {
            size: data.len() as u64,
            extents,
        };

        Ok(checksum)
    }

    fn read_chunk(&self, id: Self::ChunkId) -> io::Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    fn free_chunk(&self, id: Self::ChunkId) -> io::Result<()> {
        unimplemented!()
    }

    fn write_metadata(&mut self, metadata: &[u8]) -> io::Result<()> {
        self.superblock.metadata = metadata.to_vec();
        self.write_superblock()
    }

    fn read_metadata(&self) -> io::Result<Vec<u8>> {
        Ok(self.superblock.metadata.to_vec())
    }
}
