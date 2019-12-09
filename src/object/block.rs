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
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

use rmp_serde::{from_read, to_vec};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Result;

use super::compression::Compression;
use super::encryption::Encryption;

/// The offset of the primary superblock from the start of the file.
const SUPERBLOCK_OFFSET: u64 = 0;

/// The offset of the backup superblock from the start of the file.
const SUPERBLOCK_BACKUP_OFFSET: u64 = 4096;

/// The number of bytes reserved for the superblock and its backup.
const RESERVED_SPACE: usize = 4096 * 2;

/// An object for locating a block of data in a repository.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlockAddress(u64);

/// A sequence of contiguous blocks in the repository.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct Extent {
    /// The address of the first block in the extent.
    pub start: BlockAddress,

    /// The number of blocks in the extent.
    pub blocks: u64,
}

/// The repository's superblock.
///
/// This stores unencrypted metadata about the repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuperBlock {
    /// The unique ID of this repository.
    pub id: Uuid,

    /// The block size of the repository in bytes.
    pub block_size: u64,

    /// The number of bits that define a chunk boundary.
    ///
    /// The average size of a chunk will be 2^`chunker_bits` bytes.
    pub chunker_bits: u32,

    /// The compression method being used in this repository.
    pub compression: Compression,

    /// The encryption method being used in this repository.
    pub encryption: Encryption,

    /// The extent which stores the repository's header.
    pub header: Extent,
}

impl SuperBlock {
    /// Read the superblock from the given `file` stored at the given `offset`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Deserialize`: The superblock could not be deserialized.
    fn read_at(file: &mut File, offset: u64) -> Result<Self> {
        file.seek(SeekFrom::Start(offset))?;

        // Get the size of the superblock.
        let mut size_buffer = [0u8; size_of::<u32>()];
        file.read_exact(&mut size_buffer)?;
        let superblock_size = u32::from_be_bytes(size_buffer) as u64;

        // Deserialize the superblock.
        Ok(from_read(file.take(superblock_size))?)
    }

    /// Write the superblock to the given `file` at the given `offset`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Serialize`: The superblock could not be serialized.
    fn write_at(&self, file: &mut File, offset: u64) -> Result<()> {
        file.seek(SeekFrom::Start(offset))?;

        // Serialize the superblock.
        let superblock = to_vec(&self)?;
        let superblock_size = superblock.len();

        // Write the superblock size and the superblock itself.
        file.write_all(&superblock_size.to_be_bytes())?;
        file.write_all(&superblock)?;

        Ok(())
    }

    /// Read the superblock from the given `file` or the backup superblock if it is corrupt.
    pub fn read(file: &mut File) -> Result<Self> {
        Self::read_at(file, SUPERBLOCK_OFFSET)
            .or_else(|_| Self::read_at(file, SUPERBLOCK_BACKUP_OFFSET))
    }

    /// Write this superblock to the given `file` twice, a primary and a backup.
    pub fn write(&self, file: &mut File) -> Result<()> {
        self.write_at(file, SUPERBLOCK_OFFSET)?;
        self.write_at(file, SUPERBLOCK_BACKUP_OFFSET)?;

        Ok(())
    }
}
