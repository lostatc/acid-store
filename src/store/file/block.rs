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

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::mem::size_of;

use serde::{Deserialize, Serialize};

/// The number of bytes at the beginning of the file which are reserved.
const RESERVED_SPACE: usize = size_of::<u64>() + size_of::<u32>();

/// Appends to the given `file` to pad it to a multiple of `block_size`.
///
/// This returns the new size of the file.
pub fn pad_to_block_size(mut file: &File, block_size: u32) -> io::Result<u64> {
    let position = file.seek(SeekFrom::End(0))?;
    let padding_size = block_size as u64 - ((position - RESERVED_SPACE) % block_size as u64);
    let padding = vec![0u8; padding_size as usize];
    file.write_all(&padding)?;

    Ok(position + padding_size)
}

/// A sequence of contiguous blocks in the archive.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub struct Extent {
    /// The index of the first block in the extent.
    pub index: u64,

    /// The number of blocks in the extent.
    pub blocks: u64,
}

impl Extent {
    /// The offset of start of the extent from the start of the archive in bytes.
    pub fn start(&self, block_size: u32) -> u64 {
        RESERVED_SPACE as u64 + (self.index * block_size as u64)
    }

    /// The offset of end of the extent from the start of the archive in bytes.
    pub fn end(&self, block_size: u32) -> u64 {
        self.start(block_size) + self.length(block_size)
    }

    /// The length of the extent in bytes.
    pub fn length(&self, block_size: u32) -> u64 {
        self.blocks * block_size as u64
    }

    /// Returns the extent that is between this extent and `other`, or `None` if they are adjacent.
    pub fn between(&self, other: Extent) -> Option<Extent> {
        let new_extent = Extent {
            index: self.index + self.blocks,
            blocks: other.index - (self.index + self.blocks),
        };

        if new_extent.blocks <= 0 {
            None
        } else {
            Some(new_extent)
        }
    }
}

/// A chunk of data in an archive.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Chunk {
    /// The total size of the chunk in bytes.
    pub size: u64,

    /// The extents containing the data for this chunk.
    pub extents: Vec<Extent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuperBlock {
    pub block_size: u32,
    pub used_extents: HashSet<Extent>,
    pub metadata: Vec<u8>,
}
