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

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use relative_path::RelativePath;

use crate::archive_entry::{EntryData, EntryMetadata};
use crate::block::{Block, BlockAddress, BlockDigest, Checksum, pad_to_block_size};
use crate::error::Result;
use crate::header::{Header, HeaderAddress};
use crate::header_entry::HeaderEntryType;

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

    /// Writes the archive's header to disk, committing any changes which have been made.
    fn write_header(&mut self) -> Result<()> {
        let new_address = self.header.write(&self.path)?;
        self.header_address = new_address;
        Ok(())
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

    /// Writes the data from the given `source` to the archive.
    ///
    /// This returns the `EntryType::File` for the written data.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    fn write_file(&mut self, mut source: &mut impl Read) -> Result<HeaderEntryType> {
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

        let entry = HeaderEntryType::File {
            size: block_digest.bytes_read(),
            checksum: block_digest.result(),
            blocks: addresses,
        };

        Ok(entry)
    }

    /// Writes the data stored at the given `addresses` to `dest`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    fn read_file(&self, addresses: Vec<BlockAddress>, dest: &mut impl Write) -> Result<()> {
        let mut archive = File::open(&self.path)?;

        for block_address in addresses {
            let block = block_address.read_block(&mut archive)?;
            dest.write_all(block.data())?;
        }

        Ok(())
    }

    /// Returns an iterator of archive entries which are children of `parent`.
    pub fn list(parent: &RelativePath) -> Result<Box<dyn Iterator<Item=&EntryMetadata>>> {
        unimplemented!()
    }

    /// Returns an iterator of archive entries which are descendants of `parent`.
    pub fn walk(parent: &RelativePath) -> Result<Box<dyn Iterator<Item=&EntryMetadata>>> {
        unimplemented!()
    }

    /// Get the data for the archive entry at `path`.
    pub fn data(path: &RelativePath) -> Result<&EntryData> {
        unimplemented!()
    }

    /// Get the metadata for the archive entry at `path`.
    pub fn metadata(path: &RelativePath) -> Result<&EntryMetadata> {
        unimplemented!()
    }

    /// Add a regular file entry to the archive with the given `metadata` and `contents`.
    pub fn add_file(metadata: EntryMetadata, contents: impl Read) -> Result<()> {
        unimplemented!()
    }

    /// Add a directory entry to the archive with the given `metadata`.
    pub fn add_directory(metadata: EntryMetadata) -> Result<()> {
        unimplemented!()
    }

    /// Add a symbolic link entry to the archive with the given `metadata` and `target`.
    pub fn add_link(metadata: EntryMetadata, target: PathBuf) -> Result<()> {
        unimplemented!()
    }

    /// Create an archive entry at `dest` from the file at `source`.
    ///
    /// This does not remove the `source` file from the file system.
    pub fn archive(source: &Path, dest: &RelativePath) -> Result<()> {
        unimplemented!()
    }

    /// Create a tree of archive entries at `dest` from the directory tree at `source`.
    ///
    /// This does not remove the `source` directory or its descendants from the file system.
    pub fn archive_tree(source: &Path, dest: &RelativePath) -> Result<()> {
        unimplemented!()
    }

    /// Create a file at `dest` from the archive entry at `source`.
    ///
    /// This does not remove the `source` entry from the archive.
    pub fn extract(source: &RelativePath, dest: &Path) -> Result<()> {
        unimplemented!()
    }

    /// Create a directory tree at `dest` from the tree of archive entries at `source`.
    ///
    /// This does not remove the `source` entry or its descendants from the archive.
    pub fn extract_tree(source: &RelativePath, dest: &Path) -> Result<()> {
        unimplemented!()
    }

    /// Delete the entry in the archive with the given `path`.
    pub fn delete(path: &RelativePath) -> Result<()> {
        unimplemented!()
    }

    /// Reduce the archive size by reclaiming unallocated space.
    pub fn compact() -> Result<()> {
        unimplemented!()
    }

    /// Degrament the archive by rewriting its contents.
    pub fn defragment() -> Result<()> {
        unimplemented!()
    }
}
