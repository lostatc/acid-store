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
use std::io::{copy, Read, Seek, SeekFrom};
use std::path::PathBuf;

use crate::error::Result;
use crate::header::{Header, HeaderLocation, unused_blocks};
use crate::io::{Block, block_range, BLOCK_SIZE, pad_to_block_size};

pub struct Archive {
    path: PathBuf,
    header: Header,
    location: HeaderLocation,
}

// TODO: Don't add a file if a file with the same checksum already exists.

impl Archive {
    /// Writes the data from the given `file` to the archive.
    ///
    /// This returns the addresses of the file's blocks in the archive.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    fn write_file_data(&self, file: &mut File) -> Result<Vec<Block>> {
        let mut archive = File::open(&self.path)?;
        let unused_blocks = unused_blocks(&self.header, &self.location);

        // Fill unused blocks in the archive first.
        for block in &unused_blocks {
            archive.seek(SeekFrom::Start(block.address()))?;
            copy(&mut file.try_clone()?.take(BLOCK_SIZE as u64), &mut archive)?;
        }

        // Append remaining data to the end of the archive.
        pad_to_block_size(&mut archive)?;
        let first_new_block = archive.seek(SeekFrom::End(0))?;
        copy(file, &mut archive)?;
        let end_of_file = archive.seek(SeekFrom::Current(0))?;

        // Get the locations of the file's blocks.
        let mut blocks = unused_blocks.clone();
        blocks.extend(block_range(first_new_block, end_of_file));

        Ok(blocks)
    }
}
