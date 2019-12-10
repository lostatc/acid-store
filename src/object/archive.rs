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
use std::hash::Hash;
use std::io::{self, Read, Seek, SeekFrom, Write};

use num_integer::div_floor;

use crate::error::Result;

use super::block::{allocate_extents, Extent, pad_to_block_size, SuperBlock};
use super::chunk::Chunk;
use super::encryption::Key;
use super::header::Header;
use super::object::{Checksum, compute_checksum};

struct ObjectArchive<K>
where
    K: Eq + Hash + Clone,
{
    /// The superblock for this archive.
    superblock: SuperBlock,

    /// The header for this archive.
    header: Header<K>,

    /// The file handle for the archive.
    archive_file: File,

    /// The encryption key for the repository.
    key: Key,
}

impl<K> ObjectArchive<K>
where
    K: Eq + Hash + Clone,
{
    /// Returns the data in the given `extent`.
    fn read_extent(&mut self, extent: Extent) -> io::Result<Vec<u8>> {
        self.archive_file
            .seek(SeekFrom::Start(extent.start(self.superblock.block_size)))?;

        let mut buffer = Vec::new();

        self.archive_file
            .try_clone()?
            .take(extent.length(self.superblock.block_size))
            .read_to_end(&mut buffer)?;

        Ok(buffer)
    }

    /// Writes the given `data` to the given `extent` and returns the number of bytes written.
    fn write_extent(&mut self, extent: Extent, data: &[u8]) -> io::Result<usize> {
        self.archive_file
            .seek(SeekFrom::Start(extent.start(self.superblock.block_size)))?;

        let bytes_written = min(
            data.len(),
            extent.length(self.superblock.block_size) as usize,
        );

        self.archive_file.write_all(&data[..bytes_written])?;

        Ok(bytes_written)
    }

    /// Creates a new extent at the end of the file and returns it.
    fn new_extent(&mut self) -> io::Result<Extent> {
        let offset = pad_to_block_size(&mut self.archive_file, self.superblock.block_size)?;
        let index = div_floor(offset, self.superblock.block_size as u64);
        Ok(Extent {
            index,
            blocks: std::u64::MAX,
        })
    }

    /// Returns a list of extents which are unused and can be overwritten.
    fn unused_extents(&mut self) -> io::Result<Vec<Extent>> {
        // Get all extents which are part of a chunk.
        let mut all_extents = self
            .header
            .chunks
            .values()
            .flat_map(|chunk| chunk.extents.iter().copied())
            .collect::<Vec<_>>();

        // Include the extent storing the header.
        all_extents.push(self.superblock.header);

        // Sort extents by their location in the file.
        all_extents.sort_by_key(|extent| extent.index);

        // Get the extents which are unused.
        let mut unused_extents = all_extents
            .windows(2)
            .filter_map(|pair| pair[0].between(pair[1]))
            .collect::<Vec<_>>();

        // Create a new extent at the end of the file and add it.
        unused_extents.push(self.new_extent()?);

        Ok(unused_extents)
    }

    /// Compresses and encrypts the given `data`.
    fn encode_data(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let mut compressed_data = Vec::new();

        self.superblock
            .compression
            .compress(data)
            .read_to_end(&mut compressed_data)?;

        Ok(self
            .superblock
            .encryption
            .encrypt(compressed_data.as_ref(), &self.key))
    }

    /// Decrypts and decompresses the given `data`.
    fn decode_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        let decrypted_data = self.superblock.encryption.decrypt(data, &self.key)?;

        let mut decompressed_data = Vec::new();
        self.superblock
            .compression
            .decompress(decrypted_data.as_slice())
            .read_to_end(&mut decompressed_data)?;

        Ok(decompressed_data)
    }

    /// Writes the given `data` as a chunk and returns its checksum.
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<Checksum> {
        // Get a checksum of the unencoded data and then encode it.
        let checksum = compute_checksum(data);
        let encoded_data = self.encode_data(data)?;

        // Get the list of extents which will hold the data.
        let extents = allocate_extents(
            self.unused_extents()?,
            self.superblock.block_size,
            encoded_data.len() as u64,
        );

        // Write the encoded data to the extents.
        let mut bytes_written = 0;
        for extent in &extents {
            bytes_written += self.write_extent(*extent, &encoded_data[bytes_written..])?;
        }

        // Write this chunk to the archive.
        self.header.chunks.insert(
            checksum,
            Chunk {
                size: encoded_data.len() as u64,
                extents,
            },
        );

        Ok(checksum)
    }

    /// Returns the bytes of the chunk with the given checksum, or `None` if there is none.
    fn read_chunk(&mut self, checksum: Checksum) -> Result<Option<Vec<u8>>> {
        let chunk = match self.header.chunks.get(&checksum) {
            None => return Ok(None),
            Some(value) => value.clone()
        };

        // Read the contents of each extent in the chunk into a buffer.
        let mut chunk_data = Vec::new();
        for extent in chunk.extents {
            chunk_data.append(&mut self.read_extent(extent)?);
        }

        // Drop bytes which aren't part of this chunk.
        chunk_data.truncate(chunk.size as usize);

        // Decode the contents of the chunk.
        let decoded_data = self.decode_data(&chunk_data)?;

        Ok(Some(decoded_data))
    }
}
