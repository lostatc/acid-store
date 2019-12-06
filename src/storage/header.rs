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
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

use rmp_serde::{decode, encode};
use serde::{Deserialize, Serialize};

use crate::error::Result;

use super::block::{pad_to_block_size, BlockAddress, BLOCK_OFFSET};
use super::object::Object;

/// Metadata about the archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    /// A map of object names to objects which are in this archive.
    pub objects: HashMap<String, Object>,
}

impl Header {
    /// Creates a new empty header.
    pub fn new() -> Self {
        Header {
            objects: HashMap::new(),
        }
    }

    /// Returns the set of addresses of blocks used for storing data.
    pub fn data_blocks(&self) -> HashSet<BlockAddress> {
        self.objects
            .values()
            .flat_map(|object| &object.data.blocks)
            .copied()
            .collect()
    }

    /// Reads the header from the given `archive`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred reading from the archive.
    /// - `Error::Deserialize`: An error occurred deserializing the header.
    pub fn read(archive: &mut File) -> Result<(Header, HeaderAddress)> {
        let mut offset_buffer = [0u8; size_of::<u64>()];
        let archive_size = archive.metadata()?.len();

        // Get the offset of the header.
        archive.seek(SeekFrom::Start(0))?;
        archive.read_exact(&mut offset_buffer)?;
        let offset = u64::from_be_bytes(offset_buffer);

        // Read the header size and header.
        archive.seek(SeekFrom::Start(offset))?;
        archive.read_exact(&mut offset_buffer)?;
        let header_size = u64::from_be_bytes(offset_buffer);
        let header = decode::from_read(archive.take(header_size))?;

        let header_address = HeaderAddress {
            offset,
            header_size,
            archive_size,
        };
        Ok((header, header_address))
    }

    /// Writes this header to the given `archive` and returns its address.
    ///
    /// This does not overwrite the old header, but instead marks the space as unused so that it can
    /// be overwritten with new data in the future. If this method call is interrupted before the
    /// header is fully written, the old header will still be valid and the written bytes of the new
    /// header will be marked as unused.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred writing to the archive.
    /// - `Error::Serialize`: An error occurred serializing the header.
    pub fn write(&self, mut archive: &mut File) -> Result<HeaderAddress> {
        // Pad the file to a multiple of `BLOCK_SIZE`.
        let offset = pad_to_block_size(&mut archive)?;

        // Append the new header size and header.
        let serialized_header = encode::to_vec(&self)?;
        let header_size = serialized_header.len() as u64;
        archive.write_all(&header_size.to_be_bytes())?;
        archive.write_all(&serialized_header)?;

        // Update the header offset to point to the new header.
        archive.seek(SeekFrom::Start(0))?;
        archive.write_all(&offset.to_be_bytes())?;

        Ok(HeaderAddress {
            offset,
            header_size,
            archive_size: archive.metadata()?.len(),
        })
    }
}

/// The address of the header in the archive.
#[derive(Debug, PartialEq, Eq)]
pub struct HeaderAddress {
    /// The byte offset of the first block in the header.
    offset: u64,

    /// The size of the header in bytes.
    ///
    /// This does not include the 8 bytes at the start of the header which store the header size.
    header_size: u64,

    /// The size of the archive in bytes.
    archive_size: u64,
}

impl HeaderAddress {
    /// Returns the list of addresses of all blocks in the archive.
    pub fn blocks(&self) -> Vec<BlockAddress> {
        BlockAddress::range(BLOCK_OFFSET, self.archive_size)
    }

    /// Returns the list of addresses of blocks used for storing the header.
    pub fn header_blocks(&self) -> Vec<BlockAddress> {
        BlockAddress::range(
            self.offset,
            self.offset + size_of::<u64>() as u64 + self.header_size,
        )
    }
}
