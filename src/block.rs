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
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// The size of the checksum of the block data.
pub const BLOCK_HASH_SIZE: usize = 32;

/// The size of the data contained in the block.
pub const BLOCK_DATA_SIZE: usize = 4096;

/// The total size of the block.
pub const BLOCK_SIZE: usize = BLOCK_HASH_SIZE + BLOCK_DATA_SIZE;

/// The checksum of a block.
type BlockChecksum = [u8; BLOCK_HASH_SIZE];

/// The data of a block.
type BlockData = [u8; BLOCK_DATA_SIZE];

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

    /// Returns the `BlockAddress` of the block at the byte `offset` from the start of the archive.
    ///
    /// The `offset` doesn't have to be on a block boundary. It can be anywhere between the start
    /// and end of the block.
    pub fn from_offset(offset: u64) -> Self {
        // The first bytes in the archive are the offset of the header.
        let start = size_of::<u64>() as u64;
        let quotient = (offset - start) / BLOCK_SIZE as u64;
        let is_boundary = (offset - start) % BLOCK_SIZE as u64 == 0;
        let index = if is_boundary { quotient - 1 } else { quotient };

        BlockAddress(index as u32)
    }

    /// Returns the range of `BlockAddress` values between `start` and `end`.
    pub fn range(start: BlockAddress, end: BlockAddress) -> Vec<BlockAddress> {
        (start.0..=end.0).map(BlockAddress).collect()
    }

    /// Returns the byte offset of the block from the beginning of the file.
    fn offset(self) -> u64 {
        // The first bytes in the archive are the offset of the header.
        size_of::<u64>() as u64 + (self.0 as u64 * BLOCK_SIZE as u64)
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
    let file_size = file.metadata()?.len();
    let padding_size = file_size % BLOCK_SIZE as u64;
    let padding = vec![0u8; padding_size as usize];
    file.write_all(&padding)?;

    Ok(())
}
