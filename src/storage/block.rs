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

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::iter;
use std::mem::size_of;

use crypto::blake2b::Blake2b;
use num_integer::{div_ceil, div_floor};
use serde::{Deserialize, Serialize};

/// The size of a checksum.
pub const CHECKSUM_SIZE: usize = 32;

/// The size of the integer specifying the number of bytes in the block.
const BLOCK_LENGTH_SIZE: usize = size_of::<u16>();

/// The size of the block's data buffer.
const BLOCK_BUFFER_SIZE: usize = 4096;

/// The total size of a block.
pub const BLOCK_SIZE: usize = CHECKSUM_SIZE + BLOCK_LENGTH_SIZE + BLOCK_BUFFER_SIZE;

/// The number of bytes between the start of the archive and the first block.
///
/// The first `BLOCK_OFFSET` bytes in the archive store the offset of the header.
pub const BLOCK_OFFSET: u64 = size_of::<u64>() as u64;

/// A checksum.
pub type Checksum = [u8; CHECKSUM_SIZE];

/// A block of data.
pub struct Block {
    /// The BLAKE2 checksum of the block's data.
    pub checksum: Checksum,

    /// The number of bytes in the block.
    pub size: usize,

    /// The buffer which stores the block's data.
    pub buffer: [u8; BLOCK_BUFFER_SIZE],
}

impl Block {
    /// The data contained in this block.
    pub fn data(&self) -> &[u8] {
        &self.buffer[..self.size as usize]
    }

    /// Creates a `Block` by reading bytes from the given `source`.
    ///
    /// This returns `Some` if the block was read from `source`, or `None` if `source` was at EOF.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn from_read(source: &mut impl Read) -> io::Result<Option<Self>> {
        let mut buffer = [0u8; BLOCK_BUFFER_SIZE];
        let bytes_read = read_all(source, &mut buffer)?;

        if bytes_read == 0 {
            Ok(None)
        } else {
            let mut checksum = [0u8; CHECKSUM_SIZE];
            Blake2b::blake2b(&mut checksum, &buffer[..bytes_read], &[0u8; 0]);
            Ok(Some(Block {
                checksum,
                size: bytes_read,
                buffer,
            }))
        }
    }

    /// Returns an iterator over the blocks in the given `source` file.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn iter_blocks<'a>(
        source: &'a mut impl Read,
    ) -> impl Iterator<Item = io::Result<Self>> + 'a {
        iter::from_fn(move || match Self::from_read(source) {
            Ok(option) => option.map(Ok),
            Err(error) => Some(Err(error)),
        })
    }

    /// Writes the contents of this block to the given `destination`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write(&self, destination: &mut impl Write) -> io::Result<()> {
        destination.write_all(&self.checksum)?;
        destination.write_all(&(self.size as u16).to_be_bytes())?;
        destination.write_all(&self.buffer)?;
        Ok(())
    }

    /// Writes the contents of this block to the given `address` in an `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write_at(&self, archive: &mut File, address: BlockAddress) -> io::Result<()> {
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
        (start_index..end_index)
            .map(|index| BlockAddress(index as u32))
            .collect()
    }

    /// Returns the byte offset of the start of the block from the beginning of the file.
    pub fn offset(self) -> u64 {
        BLOCK_OFFSET + (self.0 as u64 * BLOCK_SIZE as u64)
    }

    /// Reads the block's checksum from the given `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read_checksum(self, archive: &mut File) -> io::Result<Checksum> {
        let mut checksum = [0u8; CHECKSUM_SIZE];
        archive.seek(SeekFrom::Start(self.offset()))?;
        archive.read_exact(&mut checksum)?;
        Ok(checksum)
    }

    /// Read the block from the given `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read_block(self, archive: &mut File) -> io::Result<Block> {
        let mut length_buffer = [0u8; BLOCK_LENGTH_SIZE];
        let mut buffer = [0u8; BLOCK_BUFFER_SIZE];

        archive.seek(SeekFrom::Start(self.offset() + CHECKSUM_SIZE as u64))?;
        archive.read_exact(&mut length_buffer)?;
        archive.read_exact(&mut buffer)?;

        let checksum = self.read_checksum(archive)?;
        let size = u16::from_be_bytes(length_buffer) as usize;
        Ok(Block {
            checksum,
            size,
            buffer,
        })
    }

    /// Returns a new reader for reading the contents of the block at this address.
    pub fn new_reader(self, archive: &mut File) -> io::Result<impl Read> {
        archive.seek(SeekFrom::Start(self.offset() + CHECKSUM_SIZE as u64))?;

        let mut length_buffer = [0u8; BLOCK_LENGTH_SIZE];
        archive.read_exact(&mut length_buffer)?;
        let size = u16::from_be_bytes(length_buffer) as u64;

        Ok(archive.try_clone()?.take(size))
    }
}

/// A `Read` which counts the total number of bytes read from it.
pub struct CountingReader<T: Read> {
    reader: T,
    bytes_read: u64,
}

impl<T: Read> CountingReader<T> {
    /// Creates a new `CountingReader` which wraps the given `reader`.
    pub fn new(reader: T) -> Self {
        CountingReader {
            reader,
            bytes_read: 0,
        }
    }

    /// The total number of bytes which have been read from the reader.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl<T: Read> Read for CountingReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = self.reader.read(buf)?;
        self.bytes_read += bytes_read as u64;
        Ok(bytes_read)
    }
}

/// Read bytes from `source` until `buffer` is full or EOF.
///
/// This returns the number of bytes read.
///
/// # Errors
/// - `Error::Io`: An I/O error occurred.
pub fn read_all(source: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut bytes_read;
    let mut total_read = 0;

    loop {
        bytes_read = source.read(&mut buffer[total_read..])?;
        if bytes_read == 0 {
            break;
        }
        total_read += bytes_read;
    }

    Ok(total_read)
}

/// Writes to the given `file` to pad it to a multiple of `BLOCK_SIZE`.
///
/// This function assumes that the cursor is already at the end of the file. This returns the seek
/// position after the padding bytes have been written.
///
/// # Errors
/// - `Error::Io`: An I/O error occurred.
pub fn pad_to_block_size(file: &mut File) -> io::Result<u64> {
    let position = file.seek(SeekFrom::Current(0))?;
    let padding_size = (position - BLOCK_OFFSET) % BLOCK_SIZE as u64;
    let padding = vec![0u8; padding_size as usize];
    file.write_all(&padding)?;

    Ok(position + padding_size)
}
