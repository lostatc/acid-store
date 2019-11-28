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

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter;
use std::mem::size_of;

use crypto::blake2b::Blake2b;
use crypto::digest::Digest;
use num_integer::{div_ceil, div_floor};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::header::{FILE_HASH_SIZE, FileChecksum};

/// The size of the checksum of the block data.
pub const BLOCK_HASH_SIZE: usize = 32;

/// The size of the data contained in the block.
pub const BLOCK_DATA_SIZE: usize = 4096;

/// The total size of the block.
pub const BLOCK_SIZE: usize = BLOCK_HASH_SIZE + BLOCK_DATA_SIZE;

/// The number of bytes between the start of the archive and the first block.
///
/// The first `BLOCK_OFFSET` bytes in the archive store the offset of the header.
pub const BLOCK_OFFSET: u64 = size_of::<u64>() as u64;

/// The checksum of a block.
pub type BlockChecksum = [u8; BLOCK_HASH_SIZE];

/// The data of a block.
pub type BlockData = [u8; BLOCK_DATA_SIZE];

/// A block of data.
pub struct Block {
    /// The BLAKE2 checksum of `data`.
    pub checksum: BlockChecksum,

    /// The data stored in this block.
    pub data: BlockData,
}

impl Block {
    /// Creates a `Block` by reading bytes from the given `source`.
    ///
    /// This returns `Some` if the block was read from `source`, or `None` if `source` was at EOF.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn from_read(source: &mut impl Read) -> Result<Option<Self>> {
        let mut data = [0u8; BLOCK_DATA_SIZE];
        let bytes_read = read_all(source, &mut data)?;

        if bytes_read == 0 {
            Ok(None)
        } else {
            let mut checksum = [0u8; BLOCK_HASH_SIZE];
            Blake2b::blake2b(&mut checksum, &data[..bytes_read], &[0u8; 0]);
            Ok(Some(Block { checksum, data }))
        }
    }

    /// Returns an iterator over the blocks in the given `source` file.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn iter_blocks<'a>(source: &'a mut impl Read) -> impl Iterator<Item=Result<Self>> + 'a {
        iter::from_fn(move || {
            match Self::from_read(source) {
                Ok(option) => option.map(Ok),
                Err(error) => Some(Err(error))
            }
        })
    }

    /// Writes the contents of this block to the given `destination`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write(&self, destination: &mut impl Write) -> Result<()> {
        destination.write_all(&self.checksum)?;
        destination.write_all(&self.data)?;
        Ok(())
    }

    /// Writes the contents of this block to the given `address` in an `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write_at(&self, archive: &mut File, address: BlockAddress) -> Result<()> {
        archive.seek(SeekFrom::Start(address.offset()))?;
        self.write(archive)
    }
}

/// The address of a block of data in an archive.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlockAddress(u32);

impl BlockAddress {
    /// Returns the `BlockAddress` of the block at the given `index`.
    pub fn from_index(index: u32) -> Self {
        BlockAddress(index)
    }

    /// Returns the `BlockAddress` of the block at the given `offset`.
    pub fn from_offset(offset: u64) -> Self {
        let index = div_floor(offset - BLOCK_OFFSET, BLOCK_SIZE as u64);
        BlockAddress(index as u32)
    }

    /// Returns the range of `BlockAddress` values between `start_offset` and `end_offset`.
    pub fn range(start_offset: u64, end_offset: u64) -> Vec<BlockAddress> {
        let start_index = div_floor(start_offset - BLOCK_OFFSET, BLOCK_SIZE as u64);
        let end_index = div_ceil(end_offset - BLOCK_OFFSET, BLOCK_SIZE as u64);
        (start_index..end_index).map(|index| BlockAddress(index as u32)).collect()
    }

    /// Returns the byte offset of the block from the beginning of the file.
    fn offset(self) -> u64 {
        // The first bytes in the archive are the offset of the header.
        BLOCK_OFFSET + (self.0 as u64 * BLOCK_SIZE as u64)
    }

    /// Reads the block's checksum from the given `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read_checksum(self, archive: &mut File) -> Result<BlockChecksum> {
        let mut checksum = [0u8; BLOCK_HASH_SIZE];
        archive.seek(SeekFrom::Start(self.offset()))?;
        archive.read_exact(&mut checksum)?;
        Ok(checksum)
    }

    /// Reads the block's data from the given `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read_data(self, archive: &mut File) -> Result<BlockData> {
        let mut data = [0u8; BLOCK_DATA_SIZE];
        archive.seek(SeekFrom::Start(self.offset() + BLOCK_HASH_SIZE as u64))?;
        archive.read_exact(&mut data)?;
        Ok(data)
    }

    /// Read the block from the given `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read_block(self, archive: &mut File) -> Result<Block> {
        Ok(Block { checksum: self.read_checksum(archive)?, data: self.read_data(archive)? })
    }
}

/// An `Iterator` which computes the checksum of all the blocks which pass through it.
pub struct BlockDigest {
    digest: Blake2b,
    blocks: Box<dyn Iterator<Item=Result<Block>>>,
}

impl BlockDigest {
    /// Creates a new `BlockDigest` which wraps an existing iterator.
    pub fn new(iter: impl Iterator<Item=Result<Block>> + 'static) -> Self {
        BlockDigest { digest: Blake2b::new(FILE_HASH_SIZE), blocks: Box::new(iter) }
    }

    /// Returns the checksum of all the data which has passed through the iterator so far.
    pub fn result(&mut self) -> FileChecksum {
        let mut checksum = [0u8; FILE_HASH_SIZE];
        self.digest.result(&mut checksum);
        checksum
    }
}

impl Iterator for BlockDigest {
    type Item = Result<Block>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_element = self.blocks.next();
        if let Some(Ok(block)) = &next_element {
            self.digest.input(&block.data);
        };
        next_element
    }
}

/// Read bytes from `source` until `buffer` is full or EOF.
///
/// This returns the number of bytes read.
///
/// # Errors
/// - `Error::Io`: An I/O error occurred.
fn read_all(source: &mut impl Read, buffer: &mut [u8]) -> Result<usize> {
    let mut bytes_read;
    let mut total_read = 0;

    loop {
        bytes_read = source.read(&mut buffer[total_read..])?;
        if bytes_read == 0 { break; }
        total_read += bytes_read;
    }

    Ok(total_read)
}

/// Writes to the given `file` to pad it to a multiple of `BLOCK_SIZE`.
///
/// # Errors
/// - `Error::Io`: An I/O error occurred.
pub fn pad_to_block_size(file: &mut File) -> Result<()> {
    let position = file.seek(SeekFrom::Current(0))?;
    let padding_size = (position - BLOCK_OFFSET) % BLOCK_SIZE as u64;
    let padding = vec![0u8; padding_size as usize];
    file.write_all(&padding)?;

    Ok(())
}
