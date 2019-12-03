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

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::block::{
    pad_to_block_size, Block, BlockAddress, Checksum, CountingReader, BLOCK_OFFSET,
};
use crate::entry::{ArchiveEntry, DataHandle};
use crate::error::Result;
use crate::header::{Header, HeaderAddress};

pub struct Archive {
    /// The path of the archive.
    path: PathBuf,

    /// The archive's old header.
    ///
    /// This is the archive's header as of the last time changes were committed.
    old_header: Header,

    /// The archive's current header.
    ///
    /// This is the header which will be saved when changes are committed.
    header: Header,

    /// The address of the archive's old header.
    header_address: HeaderAddress,

    /// The checksums of all the blocks in the archive and their addresses.
    block_checksums: HashMap<Checksum, BlockAddress>,
}

impl Archive {
    /// Opens the archive at the given `path`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Deserialize`: An error occurred deserializing the header.
    pub fn open(path: &Path) -> Result<Self> {
        let mut archive_file = File::open(&path)?;
        let (header, header_address) = Header::read(&mut archive_file)?;
        let mut archive = Archive {
            path: path.to_owned(),
            old_header: header.clone(),
            header,
            header_address,
            block_checksums: HashMap::new(),
        };
        archive.read_checksums()?;
        Ok(archive)
    }

    /// Creates and opens a new archive at the given `path`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Deserialize`: An error occurred deserializing the header.
    pub fn create(path: &Path) -> Result<Self> {
        let mut archive_file = File::create(&path)?;

        // Allocate space for the header address.
        archive_file.set_len(BLOCK_OFFSET)?;
        archive_file.seek(SeekFrom::End(0))?;

        let header = Header::new();
        header.write(&mut archive_file)?;

        Self::open(path)
    }

    /// Reads the checksums of the blocks in this archive and updates `block_checksums`.
    fn read_checksums(&mut self) -> io::Result<()> {
        let mut archive = File::open(&self.path)?;
        for block_address in self.header.data_blocks() {
            let checksum = block_address.read_checksum(&mut archive)?;
            self.block_checksums.insert(checksum, block_address);
        }
        Ok(())
    }

    /// Returns a list of addresses of blocks which are unused and can be overwritten.
    fn unused_blocks(&self) -> Vec<BlockAddress> {
        let mut used_blocks = HashSet::new();

        // We can't overwrite blocks which are still referenced by the old header in case the
        // changes aren't committed.
        used_blocks.extend(self.header.data_blocks());
        used_blocks.extend(self.old_header.data_blocks());
        used_blocks.extend(self.header_address.header_blocks());

        let mut unused_blocks = self.header_address.blocks();
        unused_blocks.retain(|block| !used_blocks.contains(block));

        unused_blocks
    }

    /// Writes a block to the archive.
    ///
    /// If the given `block` already exists in the archive, this method does nothing and returns the
    /// address of the existing block. Otherwise, this method adds the given `block` to the archive
    /// at the given `address` and returns that address.
    fn write_block(
        &mut self,
        mut archive: &mut File,
        block: &Block,
        address: BlockAddress,
    ) -> io::Result<BlockAddress> {
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
    fn write_unused_blocks(
        &mut self,
        mut archive: &mut File,
        blocks: &mut impl Iterator<Item = io::Result<Block>>,
    ) -> io::Result<Vec<BlockAddress>> {
        let unused_blocks = self.unused_blocks();
        let mut addresses = Vec::new();

        for block_address in unused_blocks {
            match blocks.next() {
                // Fill an unused space with the next block.
                Some(block_result) => {
                    let block = block_result?;
                    addresses.push(self.write_block(&mut archive, &block, block_address)?);
                }

                // There are no blocks left to write.
                None => break,
            }
        }

        Ok(addresses)
    }

    /// Writes the given `blocks` to the `archive` by appending to the end.
    ///
    /// This writes all remaining blocks to the archive. This returns the list of addresses that
    /// blocks have been written to.
    fn write_new_blocks(
        &mut self,
        mut archive: &mut File,
        blocks: &mut impl Iterator<Item = io::Result<Block>>,
    ) -> io::Result<Vec<BlockAddress>> {
        let mut addresses = Vec::new();

        for block_result in blocks {
            let block = block_result?;
            let block_offset = archive.seek(SeekFrom::Current(0))?;
            let block_address = BlockAddress::from_offset(block_offset);
            addresses.push(self.write_block(&mut archive, &block, block_address)?);
        }

        Ok(addresses)
    }

    /// Adds an `entry` with the given `name` to the archive.
    ///
    /// If an entry with the given `name` already existed in the archive, it is replaced and the old
    /// entry is returned. Otherwise, `None` is returned.
    pub fn insert(&mut self, name: &str, entry: ArchiveEntry) -> Option<ArchiveEntry> {
        self.header.entries.insert(name.to_string(), entry)
    }

    /// Removes and returns the entry with the given `name` from the archive.
    ///
    /// This returns `None` if there is no entry with the given `name`.
    pub fn remove(&mut self, name: &str) -> Option<ArchiveEntry> {
        self.header.entries.remove(name)
    }

    /// Returns the entry with the given `name`, or `None` if it doesn't exist.
    pub fn get(&self, name: &str) -> Option<&ArchiveEntry> {
        self.header.entries.get(name)
    }

    /// Returns a mutable reference to the entry with the given `name`, or `None` if there is none.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut ArchiveEntry> {
        self.header.entries.get_mut(name)
    }

    /// Returns the names of all the entries in this archive.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.header.entries.keys()
    }

    /// Returns a reader for reading the data associated with the given `handle`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read(&self, handle: &DataHandle) -> Result<impl Read> {
        let mut archive_file = File::open(&self.path)?;
        let mut reader: Box<dyn Read> = Box::new(io::empty());

        for block_address in &handle.blocks {
            reader = Box::new(reader.chain(block_address.new_reader(&mut archive_file)?));
        }

        Ok(reader)
    }

    /// Writes the data from `source` to the archive and returns a handle to it.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write(&mut self, source: &mut impl Read) -> Result<DataHandle> {
        let mut archive_file = OpenOptions::new().write(true).open(&self.path)?;
        let mut addresses = Vec::new();
        let mut source = CountingReader::new(source);
        let mut blocks = Block::iter_blocks(&mut source);

        // Fill unused blocks in the archive first.
        addresses.extend(self.write_unused_blocks(&mut archive_file, &mut blocks)?);

        // Pad the archive to a multiple of `BLOCK_SIZE`.
        archive_file.seek(SeekFrom::End(0))?;
        pad_to_block_size(&mut archive_file)?;

        // Append the remaining blocks to the end of the archive.
        addresses.extend(self.write_new_blocks(&mut archive_file, &mut blocks)?);

        // All the blocks have been read from the source file.
        drop(blocks);

        let handle = DataHandle {
            size: source.bytes_read(),
            blocks: addresses,
        };

        Ok(handle)
    }

    /// Commits all changes that have been made to the archive.
    ///
    /// No changes made to the archive are saved persistently until this method is called.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn commit(&mut self) -> Result<()> {
        let mut archive_file = OpenOptions::new().write(true).open(&self.path)?;
        let new_address = self.header.write(&mut archive_file)?;
        self.header_address = new_address;
        self.old_header = self.header.clone();
        Ok(())
    }

    /// Creates a copy of this archive which is compacted to reduce its size.
    ///
    /// Archives can reuse space left over from deleted entries, but they can not deallocate space
    /// which has been allocated. This means that archive files can grow in size, but never shrink.
    ///
    /// This method copies the data in this archive to a new archive, allocating the minimum amount
    /// of space necessary. This can result in a significantly smaller archive size if a lot of data
    /// has been removed from this archive and not replaced with new data.
    ///
    /// This method accepts the path of the new archive returns the newly created archive.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn compacted(&mut self, dest: &Path) -> Result<Archive> {
        let mut dest_archive = Self::create(dest)?;
        let mut dest_file = OpenOptions::new().write(true).open(dest)?;
        let mut source_file = File::open(&self.path)?;

        // Get the addresses of used blocks in this archive.
        let mut block_addresses = self.header.data_blocks();
        block_addresses.sort();

        // Lazily read blocks from this archive.
        let mut data_blocks = block_addresses
            .iter()
            .map(|address| address.read_block(&mut source_file));

        // Write blocks to the destination archive.
        dest_archive.write_new_blocks(&mut dest_file, &mut data_blocks)?;
        dest_archive.commit()?;

        Ok(dest_archive)
    }
}
