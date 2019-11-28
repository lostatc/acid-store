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
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use crate::block::{Block, BlockAddress, pad_to_block_size};
use crate::error::Result;
use crate::header::{Header, HeaderLocation, unused_blocks};

pub struct Archive {
    path: PathBuf,
    header: Header,
    location: HeaderLocation,
}

impl Archive {
    // TODO: Don't add a block if a block with the same checksum already exists.
    // TODO: Calculate the file checksum.
    /// Writes the data from the given `file` to the archive.
    ///
    /// This returns the addresses of the file's blocks in the archive.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    fn write_file_data(&self, file: &mut File) -> Result<Vec<BlockAddress>> {
        let mut archive = File::open(&self.path)?;
        let unused_blocks = unused_blocks(&self.header, &self.location);
        let mut addresses = Vec::new();

        // Fill unused blocks in the archive first.
        for block_address in unused_blocks {
            match Block::from_read(file)? {
                Some(block) => block.write_at(&mut archive, block_address)?,
                None => break
            };
            addresses.push(block_address);
        }

        // Append remaining data to the end of the archive.
        pad_to_block_size(&mut archive)?;
        let start_address = BlockAddress::from_offset(archive.seek(SeekFrom::End(0))?);
        for block in Block::iter_blocks(file) {
            block?.write(&mut archive);
        }
        let end_address = BlockAddress::from_offset(archive.seek(SeekFrom::Current(0))?);

        // Get addresses of new blocks.
        addresses.extend(BlockAddress::range(start_address, end_address));

        Ok(addresses)
    }
}
