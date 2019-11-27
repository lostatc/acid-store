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

use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::mem::size_of;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// The size of blocks used for storing data in the archive.
pub const BLOCK_SIZE: usize = 4096;

/// The location of a block of data in the archive.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Block {
    /// The index of the block in the list of blocks.
    pub index: u32
}

impl Block {
    /// The address of the block as a number of bytes from the start of the file.
    pub fn address(&self) -> u64 {
        size_of::<Block>() as u64 + (self.index as u64 * BLOCK_SIZE as u64)
    }
}

/// Returns the block indices of the blocks between the given addresses.
///
/// The `start_address` is the beginning of the first block. The `end_address` is any address
/// between the start and end of the last block.
pub fn block_range(start_address: u64, end_address: u64) -> HashSet<Block> {
    let addresses = (start_address..end_address).step_by(BLOCK_SIZE);
    (0..addresses.count()).map(|index| Block { index: index as u32 }).collect()
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
