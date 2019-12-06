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

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::Result;

use super::block::{
    pad_to_block_size, Block, BlockAddress, Checksum, CountingReader, BLOCK_OFFSET,
};
use super::header::{Header, HeaderAddress};
use super::object::{DataHandle, Object};

/// An object store which stores its data in a single file.
///
/// An `ObjectArchive` is a binary file format for efficiently storing large amounts of binary data.
/// An archive consists of objects, each of which has a unique name, metadata, and data associated
/// with it.
///
/// Data in an archive is automatically deduplicated at the block level. Changes made to an
/// `ObjectArchive` are not persisted to disk until `commit` is called.
pub struct ObjectArchive {
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

    /// A reference to the open archive file.
    archive_file: File,
}

impl ObjectArchive {
    /// Opens the archive at the given `path`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `NotFound`: The archive file does not exist.
    ///     - `PermissionDenied`: The user lack permission to open the archive file.
    /// - `Error::Deserialize`: The file is not a valid archive file.
    pub fn open(path: &Path) -> Result<Self> {
        let mut archive_file = OpenOptions::new().read(true).write(true).open(path)?;
        let (header, header_address) = Header::read(&mut archive_file)?;
        let mut archive = ObjectArchive {
            path: path.to_owned(),
            old_header: header.clone(),
            header,
            header_address,
            block_checksums: HashMap::new(),
            archive_file,
        };
        archive.read_checksums()?;
        Ok(archive)
    }

    /// Creates and opens a new archive at the given `path`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `PermissionDenied`: The user lack permission to create the archive file.
    ///     - `AlreadyExists`: A file already exists at `path`.
    pub fn create(path: &Path) -> Result<Self> {
        let mut archive_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;

        // Allocate space for the header address.
        archive_file.set_len(BLOCK_OFFSET)?;
        archive_file.seek(SeekFrom::End(0))?;

        let header = Header::new();
        header.write(&mut archive_file)?;

        Self::open(path)
    }

    /// Reads the checksums of the blocks in this archive and updates `block_checksums`.
    fn read_checksums(&mut self) -> io::Result<()> {
        for block_address in self.header.data_blocks() {
            let checksum = block_address.read_checksum(&mut self.archive_file)?;
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
    fn write_block(&mut self, block: &Block, address: BlockAddress) -> io::Result<BlockAddress> {
        // Check if the block already exists in the archive.
        match self.block_checksums.get(&block.checksum) {
            // Use the address of the existing block.
            Some(existing_address) => Ok(*existing_address),

            // Add the new block.
            None => {
                block.write_at(&mut self.archive_file, address)?;
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
        blocks: &mut impl Iterator<Item = io::Result<Block>>,
    ) -> io::Result<Vec<BlockAddress>> {
        let unused_blocks = self.unused_blocks();
        let mut addresses = Vec::new();

        for block_address in unused_blocks {
            match blocks.next() {
                // Fill an unused space with the next block.
                Some(block_result) => {
                    let block = block_result?;
                    addresses.push(self.write_block(&block, block_address)?);
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
        blocks: &mut impl Iterator<Item = io::Result<Block>>,
    ) -> io::Result<Vec<BlockAddress>> {
        let mut addresses = Vec::new();

        for block_result in blocks {
            let block = block_result?;
            let block_offset = self.archive_file.seek(SeekFrom::Current(0))?;
            let block_address = BlockAddress::from_offset(block_offset);
            addresses.push(self.write_block(&block, block_address)?);
        }

        Ok(addresses)
    }

    /// Adds an `object` with the given `name` to the archive.
    ///
    /// If an object with the given `name` already existed in the archive, it is replaced and the
    /// old object is returned. Otherwise, `None` is returned.
    pub fn insert(&mut self, name: &str, object: Object) -> Option<Object> {
        self.header.objects.insert(name.to_string(), object)
    }

    /// Removes and returns the object with the given `name` from the archive.
    ///
    /// This returns `None` if there is no object with the given `name`.
    pub fn remove(&mut self, name: &str) -> Option<Object> {
        self.header.objects.remove(name)
    }

    /// Returns the object with the given `name`, or `None` if it doesn't exist.
    pub fn get(&self, name: &str) -> Option<&Object> {
        self.header.objects.get(name)
    }

    /// Returns a mutable reference to the object with the given `name`, or `None` if there is none.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Object> {
        self.header.objects.get_mut(name)
    }

    /// Returns an iterator over all the names of objects in this archive.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.header.objects.keys()
    }

    /// Returns an iterator over all the names and objects in this archive.
    pub fn objects(&self) -> impl Iterator<Item = (&String, &Object)> {
        self.header.objects.iter()
    }

    /// Returns a reader for reading the data associated with the given `handle`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read(&mut self, handle: &DataHandle) -> Result<impl Read> {
        let mut reader: Box<dyn Read> = Box::new(io::empty());
        for block_address in &handle.blocks {
            reader = Box::new(reader.chain(block_address.new_reader(&mut self.archive_file)?));
        }

        Ok(reader)
    }

    /// Writes the data from `source` to the archive and returns a handle to it.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write(&mut self, source: &mut impl Read) -> Result<DataHandle> {
        let mut addresses = Vec::new();
        let mut source = CountingReader::new(source);
        let mut blocks = Block::iter_blocks(&mut source);

        // Fill unused blocks in the archive first.
        addresses.extend(self.write_unused_blocks(&mut blocks)?);

        // Pad the archive to a multiple of `BLOCK_SIZE`.
        self.archive_file.seek(SeekFrom::End(0))?;
        pad_to_block_size(&mut self.archive_file)?;

        // Append the remaining blocks to the end of the archive.
        addresses.extend(self.write_new_blocks(&mut blocks)?);

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
    /// No changes made to the archive are saved persistently until this method is called. If data
    /// has been written to the archive with `write` and the `ObjectArchive` is dropped before this
    /// method is called, that data will be inaccessible and will be overwritten by new data.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn commit(&mut self) -> Result<()> {
        let new_address = self.header.write(&mut self.archive_file)?;
        self.header_address = new_address;
        self.old_header = self.header.clone();
        Ok(())
    }

    /// Creates a copy of this archive which is compacted to reduce its size.
    ///
    /// Archives can reuse space left over from deleted objects, but they can not deallocate space
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
    ///     - `PermissionDenied`: The user lack permission to create the new archive.
    ///     - `AlreadyExists`: A file already exists at `dest`.
    pub fn compacted(&mut self, dest: &Path) -> Result<ObjectArchive> {
        let mut dest_archive = Self::create(dest)?;

        // Get the addresses of used blocks in this archive.
        // Sort them so they'll be in the same order in the new archive.
        let mut block_addresses = self.header.data_blocks().into_iter().collect::<Vec<_>>();
        block_addresses.sort();

        // Lazily read blocks from this archive.
        let mut data_blocks = block_addresses
            .iter()
            .map(|address| address.read_block(&mut self.archive_file));

        // Write blocks to the destination archive.
        dest_archive.write_new_blocks(&mut data_blocks)?;
        dest_archive.commit()?;

        Ok(dest_archive)
    }
}
