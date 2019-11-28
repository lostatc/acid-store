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
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use crypto::blake2b::Blake2b;
use crypto::digest::Digest;

use crate::block::{Block, BlockAddress, pad_to_block_size};
use crate::error::Result;
use crate::header::{EntryType, FILE_HASH_SIZE, Header, HeaderAddress};

pub struct Archive {
    path: PathBuf,
    header: Header,
    header_address: HeaderAddress,
}

impl Archive {
    // TODO: Don't add a block if a block with the same checksum already exists.
    /// Writes the data from the given regular `file` to the archive.
    ///
    /// This returns the `EntryType::File` for the file.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    fn write_file_data(&self, file: &mut File) -> Result<EntryType> {
        let mut archive = File::open(&self.path)?;
        let unused_blocks = self.header.unused_blocks(&self.header_address);

        let mut file_digest = Blake2b::new(FILE_HASH_SIZE);
        let mut addresses = Vec::new();

        // Fill unused blocks in the archive first.
        for block_address in unused_blocks {
            match Block::from_read(file)? {
                Some(block) => {
                    file_digest.input(&block.data);
                    block.write_at(&mut archive, block_address)?;
                    addresses.push(block_address);
                },
                None => break
            };
        }

        // Append remaining blocks to the end of the archive.
        let start_offset = archive.seek(SeekFrom::End(0))?;
        pad_to_block_size(&mut archive)?;
        for block_result in Block::iter_blocks(file) {
            let block = block_result?;
            file_digest.input(&block.data);
            block.write(&mut archive)?;
        }
        let end_offset = archive.seek(SeekFrom::Current(0))?;

        // Get file size, checksum, and block addresses.
        let file_size = file.metadata()?.len();
        let mut checksum = [0u8; FILE_HASH_SIZE];
        file_digest.result(&mut checksum);
        addresses.extend(BlockAddress::range(start_offset, end_offset));

        Ok(EntryType::File { size: file_size, checksum, blocks: addresses })
    }
}
