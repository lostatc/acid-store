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
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::block::{Block, BlockAddress, BlockDigest, Checksum, pad_to_block_size};
use crate::error::Result;
use crate::header::{EntryType, Header, HeaderAddress};

pub struct Archive {
    /// The path of the archive.
    path: PathBuf,

    /// The archive's header.
    header: Header,

    /// The address of the archive's header.
    header_address: HeaderAddress,

    /// The checksums of all the blocks in the archive and their addresses.
    block_checksums: HashMap<Checksum, BlockAddress>
}

impl Archive {
    /// Opens the archive at the given `path`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Deserialize`: An error occurred deserializing the header.
    pub fn open(path: &Path) -> Result<Self> {
        let (header, header_address) = Header::read(path)?;
        let mut archive = Archive {
            path: path.to_owned(),
            header,
            header_address,
            block_checksums: HashMap::new(),
        };
        archive.read_checksums()?;
        Ok(archive)
    }

    /// Reads the checksums of the blocks in this archive and updates `block_checksums`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    fn read_checksums(&mut self) -> Result<()> {
        let mut archive = File::open(&self.path)?;
        for block_address in self.header.data_blocks() {
            let checksum = block_address.read_checksum(&mut archive)?;
            self.block_checksums.insert(checksum, block_address);
        }
        Ok(())
    }

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
                // Fill an unused space with the next block.
                Some(block_result) => {
                    let block = block_result?;
                    addresses.push(self.write_block(&mut archive, &block, block_address)?);
                },

                // There are no blocks left to write.
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

    /// Writes the data from the given `source` to the archive as a regular file.
    ///
    /// This returns the `EntryType::File` for the written data.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    fn write_file(&mut self, mut source: &mut impl Read) -> Result<EntryType> {
        let mut archive = File::open(&self.path)?;
        let mut addresses = Vec::new();
        let mut block_digest = BlockDigest::new(Block::iter_blocks(&mut source));

        // Fill unused blocks in the archive first.
        addresses.extend(self.write_unused_blocks(&mut archive, &mut block_digest)?);

        // Pad the archive to a multiple of `BLOCK_SIZE`.
        archive.seek(SeekFrom::End(0))?;
        pad_to_block_size(&mut archive)?;

        // Append the remaining blocks to the end of the archive.
        addresses.extend(self.write_new_blocks(&mut archive, &mut block_digest)?);

        let entry = EntryType::File {
            size: block_digest.bytes_read(),
            checksum: block_digest.result(),
            blocks: addresses,
        };

        Ok(entry)
    }

    /// Writes the archive's header to disk, committing any changes which have been made.
    fn write_header(&mut self) -> Result<()> {
        let new_address = self.header.write(&self.path)?;
        self.header_address = new_address;
        Ok(())
    }
}
