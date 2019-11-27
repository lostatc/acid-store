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
use std::mem::size_of;
use std::path::PathBuf;

use chrono::NaiveDateTime;

/// The size of blocks used for storing data in the archive.
const BLOCK_SIZE: usize = 4096;

/// The address of a block as the number of bytes from the start of the file.
type Address = u64;

/// A type of file which can be stored in an archive.
pub enum EntryType {
    /// A regular file with opaque contents.
    File {
        /// The size of the file in bytes.
        size: u64,

        /// The self-describing checksum of the file.
        checksum: Vec<u8>,

        /// The addresses of the blocks containing the data for this file.
        blocks: Vec<Address>,
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
pub struct ExtendedAttribute {
    /// The name of the attribute.
    pub name: String,

    /// The value of the attribute.
    pub value: Vec<u8>,
}

/// Metadata about a file which is stored in an archive.
pub struct ArchiveEntry {
    /// The path of the file in the archive.
    pub path: PathBuf,

    /// The time the file was last modified.
    pub modified_time: NaiveDateTime,

    /// The POSIX permissions bits of the file, or `None` if POSIX permissions are not applicable.
    pub permissions: Option<i32>,

    /// The file's extended attributes.
    pub attributes: Vec<ExtendedAttribute>,

    /// The type of file this entry represents.
    pub entry_type: EntryType,
}

/// Metadata about files stored in the archive.
pub struct Header {
    /// The address of the first block used to store this header.
    pub address: Address,

    /// The size of the header in bytes.
    pub header_size: u64,

    /// The entries which are stored in this archive.
    pub entries: Vec<ArchiveEntry>,
}

impl Header {
    /// Returns the set of addresses of all blocks in the archive.
    fn blocks(archive_size: u64) -> HashSet<Address> {
        // The first bytes of the file contain the address of the header.
        let start_address = size_of::<Address>() as u64;
        (start_address..archive_size).step_by(BLOCK_SIZE).collect()
    }

    /// Returns the set of addresses used for storing the header.
    fn header_blocks(&self) -> HashSet<Address> {
        (self.address..self.header_size).step_by(BLOCK_SIZE).collect()
    }

    /// Returns the set of addresses used for storing data.
    fn data_blocks(&self) -> HashSet<Address> {
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

    /// Returns the set of addresses which are unused and can be overwritten.
    ///
    /// The returned addresses are sorted in ascending order.
    pub fn unused_blocks(&self, archive_size: u64) -> Vec<Address> {
        let mut used_blocks = self.data_blocks();
        used_blocks.extend(self.header_blocks());

        let mut unused_blocks = Self::blocks(archive_size)
            .difference(&used_blocks)
            .copied()
            .collect::<Vec<Address>>();

        unused_blocks.sort();
        unused_blocks
    }
}
