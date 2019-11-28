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

use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

use crate::block::{Block, BlockAddress, BlockChecksum, BlockDigest, pad_to_block_size};
use crate::error::Result;
use crate::header::{EntryType, FILE_HASH_SIZE, Header, HeaderAddress};

pub struct Archive {
    /// The path of the archive.
    path: PathBuf,

    /// The archive's header.
    header: Header,

    /// The address of the archive's header.
    header_address: HeaderAddress,

    /// The checksums of all the blocks in the archive and their addresses.
    block_checksums: HashMap<BlockChecksum, BlockAddress>
}

impl Archive {
    /// Writes a block to the archive.
    ///
    /// If the given `block` already exists in the archive, this method does nothing and returns the
    /// address of the existing block. Otherwise, this method adds the given `block` to the archive
    /// at the given `address` and returns that address.
    ///
    /// # Errors
    /// - `Error::Io` An I/O error occurred.
    fn write_block(
        &mut self,
        mut archive: &mut File,
        block: &Block,
        address: BlockAddress,
    ) -> Result<BlockAddress> {
        // Check if the block already exists in the archive.
        match self.block_checksums.get(&block.checksum) {
            // Use the address of the existing block.
            Some(existing_address) => Ok(*existing_address),

            // Add the new block.
            None => {
                block.write_at(&mut archive, address)?;
                self.block_checksums.insert(block.checksum, address);
                Ok(address)
            }
        }
    }

    /// Writes the given `blocks` to the `archive` by filling unused spaces.
    ///
    /// This only writes as many blocks as there are unused spaces to fill. If there are no unused
    /// spaces, no blocks will be written. This returns the list of addresses that blocks have been
    /// written to.
    ///
    /// # Errors
    /// - `Error::Io` An I/O error occurred.
    fn write_unused_blocks(
        &mut self,
        mut archive: &mut File,
        blocks: &mut impl Iterator<Item=Result<Block>>,
    ) -> Result<Vec<BlockAddress>> {
        let unused_blocks = self.header.unused_blocks(&self.header_address);
        let mut addresses = Vec::new();

        for block_address in unused_blocks {
            match blocks.next() {
                Some(block_result) => {
                    let block = block_result?;
                    addresses.push(self.write_block(&mut archive, &block, block_address)?);
                },
                None => break
            }
        }

        Ok(addresses)
    }

    /// Writes the given `blocks` to the `archive` by appending to the end.
    ///
    /// This writes all remaining blocks to the archive. This returns the list of addresses that
    /// blocks have been written to.
    ///
    /// # Errors
    /// - `Error::Io` An I/O error occurred.
    fn write_new_blocks(
        &mut self,
        mut archive: &mut File,
        blocks: &mut impl Iterator<Item=Result<Block>>,
    ) -> Result<Vec<BlockAddress>> {
        let mut addresses = Vec::new();

        for block_result in blocks {
            let block = block_result?;
            let block_offset = archive.seek(SeekFrom::Current(0))?;
            let block_address = BlockAddress::from_offset(block_offset);
            addresses.push(self.write_block(&mut archive, &block, block_address)?);
        }

        Ok(addresses)
    }

    /// Writes the data from the given regular `file` to the archive.
    ///
    /// This returns the `EntryType::File` for the file.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    fn write_file_data(&mut self, file: &'static mut File) -> Result<EntryType> {
        let mut archive = File::open(&self.path)?;
        let mut addresses = Vec::new();
        let file_size = file.metadata()?.len();
        let mut block_digest = BlockDigest::new(Block::iter_blocks(file));

        // Fill unused blocks in the archive first.
        addresses.extend(self.write_unused_blocks(&mut archive, &mut block_digest)?);

        // Pad the archive to a multiple of `BLOCK_SIZE`.
        archive.seek(SeekFrom::End(0))?;
        pad_to_block_size(&mut archive)?;

        // Append the remaining blocks to the end of the archive.
        addresses.extend(self.write_new_blocks(&mut archive, &mut block_digest)?);

        Ok(EntryType::File { size: file_size, checksum: block_digest.result(), blocks: addresses })
    }
}
