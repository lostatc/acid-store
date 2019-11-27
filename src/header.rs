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
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};

use chrono::NaiveDateTime;
use rmp_serde::{decode, encode};
use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::io::{Block, block_range, pad_to_block_size};
use crate::serialization::SerializableNaiveDateTime;

/// A type of file which can be stored in an archive.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryType {
    /// A regular file with opaque contents.
    File {
        /// The size of the file in bytes.
        size: u64,

        /// The self-describing checksum of the file.
        checksum: Vec<u8>,

        /// The locations of blocks containing the data for this file.
        blocks: Vec<Block>,
    },

    /// A directory.
    Directory,

    /// A symbolic link.
    Link {
        /// The path of the target of this symbolic link.
        target: PathBuf
    },
}

/// An extended attribute of a file.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ExtendedAttribute {
    /// The name of the attribute.
    pub name: String,

    /// The value of the attribute.
    pub value: Vec<u8>,
}

/// Metadata about a file which is stored in an archive.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ArchiveEntry {
    /// The path of the file in the archive.
    pub path: PathBuf,

    /// The time the file was last modified.
    #[serde(with = "SerializableNaiveDateTime")]
    pub modified_time: NaiveDateTime,

    /// The POSIX permissions bits of the file, or `None` if POSIX permissions are not applicable.
    pub permissions: Option<i32>,

    /// The file's extended attributes.
    pub attributes: Vec<ExtendedAttribute>,

    /// The type of file this entry represents.
    pub entry_type: EntryType,
}

/// Metadata about files stored in the archive.
#[derive(Debug, Serialize, Deserialize)]
pub struct Header {
    /// The entries which are stored in this archive.
    pub entries: Vec<ArchiveEntry>,
}

impl Header {
    /// Returns the set of locations of blocks used for storing data.
    fn data_blocks(&self) -> HashSet<Block> {
        self.entries
            .iter()
            .filter_map(|entry| match &entry.entry_type {
                EntryType::File { blocks, .. } => Some(blocks),
                _ => None
            })
            .flatten()
            .copied()
            .collect::<HashSet<_>>()
    }

    /// Reads the header from the given `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred reading from the archive.
    /// - `Error::Deserialize`: An error occurred deserializing the header.
    pub fn read(archive: &Path) -> Result<(Header, HeaderLocation)> {
        let mut file = File::open(archive)?;
        let mut address_buffer = [0u8; size_of::<u64>()];
        let archive_size = file.metadata()?.len();

        // Get the address of the header.
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut address_buffer)?;
        let address = u64::from_be_bytes(address_buffer);

        // Read the header size and header.
        file.seek(SeekFrom::Start(address))?;
        file.read_exact(&mut address_buffer)?;
        let header_size = u64::from_be_bytes(address_buffer);
        let header = decode::from_read(file.take(header_size))?;

        let location = HeaderLocation { address, header_size, archive_size };
        Ok((header, location))
    }

    /// Writes this header to the given `archive` and returns its location.
    ///
    /// This does not overwrite the old header, but instead marks the space as unused so that it can
    /// be overwritten with new data in the future. If this method call is interrupted before the
    /// header is fully written, the old header will still be valid and the written bytes of the new
    /// header will be marked as unused.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred writing to the archive.
    /// - `Error::Serialize`: An error occurred serializing the header.
    pub fn write(&self, archive: &Path) -> Result<HeaderLocation> {
        let mut file = File::open(archive)?;

        // Pad the file to a multiple of `BLOCK_SIZE`.
        let address = file.seek(SeekFrom::End(0))?;
        pad_to_block_size(&mut file)?;

        // Append the new header size and header.
        let serialized_header = encode::to_vec(&self)?;
        file.write_all(&serialized_header.len().to_be_bytes())?;
        file.write_all(&serialized_header)?;

        // Update the header address to point to the new header.
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&address.to_be_bytes())?;

        let archive_size = file.metadata()?.len();
        let header_size = archive_size - address;

        Ok(HeaderLocation { address, header_size, archive_size })
    }
}

/// The location of the header in the archive.
pub struct HeaderLocation {
    /// The address of the first block in the header.
    pub address: u64,

    /// The size of the header in bytes.
    pub header_size: u64,

    /// The size of the archive in bytes.
    pub archive_size: u64,
}

impl HeaderLocation {
    /// Returns the set of locations of all blocks in the archive.
    fn blocks(&self) -> HashSet<Block> {
        // The first bytes of the file contain the address of the header.
        block_range(size_of::<Block>() as u64, self.archive_size)
    }

    /// Returns the set of locations of blocks used for storing the header.
    fn header_blocks(&self) -> HashSet<Block> {
        block_range(self.address, self.header_size)
    }
}

/// Returns a sorted list of locations of blocks which are unused and can be overwritten.
pub fn unused_blocks(header: &Header, location: &HeaderLocation) -> Vec<Block> {
    let mut used_blocks = header.data_blocks();
    used_blocks.extend(location.header_blocks());

    let mut unused_blocks = location.blocks()
        .difference(&used_blocks)
        .copied()
        .collect::<Vec<_>>();

    unused_blocks.sort();
    unused_blocks
}
