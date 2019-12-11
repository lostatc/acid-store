/*
 * Copyright 2019 Wren Powell
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
use rmp_serde::to_vec;
use serde::de::DeserializeOwned;
use serde::Serialize;

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
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
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
    ///
    /// This pads the file so that the new extent is aligned with the block size. The returned
    /// extent has a length of `std::u64::MAX`, but the space for the new extent is not allocated.
    fn new_extent(&mut self) -> io::Result<Extent> {
        let offset = pad_to_block_size(&mut self.archive_file, self.superblock.block_size)?;
        let index = div_floor(offset, self.superblock.block_size as u64);
        Ok(Extent {
            index,
            blocks: std::u64::MAX,
        })
    }

    /// Returns a list of extents which are unused and can be overwritten.
    ///
    /// The returned extents are sorted by their location in the file. The final extent in the list
    /// will be the extent at the end of the file, which has a length of `std::u64::MAX`.
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

    /// Compresses and encrypts the given `data` and returns it.
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

    /// Decrypts and decompresses the given `data` and returns it.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Verify`: The ciphertext verification failed.
    fn decode_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        let decrypted_data = self.superblock.encryption.decrypt(data, &self.key)?;

        let mut decompressed_data = Vec::new();
        self.superblock
            .compression
            .decompress(decrypted_data.as_slice())
            .read_to_end(&mut decompressed_data)?;

        Ok(decompressed_data)
    }

    /// Writes the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum is returned and no new data is
    /// written.
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<Checksum> {
        // Get a checksum of the unencoded data.
        let checksum = compute_checksum(data);

        // Check if the chunk already exists.
        if self.header.chunks.contains_key(&checksum) {
            return Ok(checksum)
        }

        // Encode the data.
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

        // Add this chunk to the header.
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
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Verify`: The ciphertext verification failed.
    fn read_chunk(&mut self, checksum: Checksum) -> Result<Option<Vec<u8>>> {
        // Get the chunk with the given checksum if it exists.
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

    /// Commit changes which have been made to the archive.
    ///
    /// No changes are saved persistently until this method is called.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn commit(&mut self) -> Result<()> {
        // Serialize and encode the header.
        let serialized_header = to_vec(&self.header)?;
        let encoded_header = self.encode_data(&serialized_header)?;

        // The entire header must be stored in a single extent.
        // Find the first extent which is large enough to hold it.
        let unused_extents = self.unused_extents()?;
        let header_extent = unused_extents
            .iter()
            .find(|extent|
                extent.length(self.superblock.block_size) >= encoded_header.len() as u64
            )
            .unwrap();

        // Write the header to the chosen extent.
        self.write_extent(*header_extent, &encoded_header)?;

        // Update the superblock to point to the new header.
        self.superblock.header = *header_extent;

        // Write the new superblock, atomically completing the commit.
        self.superblock.write(&mut self.archive_file)?;

        Ok(())
    }
}
