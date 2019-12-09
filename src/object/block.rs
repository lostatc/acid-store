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

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::compression::Compression;
use super::encryption::Encryption;

/// The offset of the superblock from the start of the file.
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
