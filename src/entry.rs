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
use std::io::{Read, Write};
use std::iter;

use serde::{Deserialize, Serialize};

use crate::archive::Archive;
use crate::block::{Block, BlockAddress, Checksum};
use crate::error::Result;

/// The 256-bit BLAKE2 checksum of an empty byte array.
const EMPTY_CHECKSUM: Checksum = [0x0e, 0x57, 0x51, 0xc0, 0x26, 0xe5, 0x43, 0xb2, 0xe8, 0xab, 0x2e, 0xb0, 0x60, 0x99, 0xda, 0xa1, 0xd1, 0xe5, 0xdf, 0x47, 0x77, 0x8f, 0x77, 0x87, 0xfa, 0xab, 0x45, 0xcd, 0xf1, 0x2f, 0xe3, 0xa8];

/// The serializable representation of an entry.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderEntry {
    /// The name of the entry.
    pub name: String,

    /// The metadata associated with this entry.
    pub metadata: HashMap<String, Vec<u8>>,

    /// The data associated with this entry.
    pub data: HeaderData,
}

/// The serializable representation of the data associated with an entry.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderData {
    /// The size of the entry's data in bytes.
    pub size: u64,

    /// The 256-bit BLAKE2 checksum of the entry's data.
    pub checksum: Checksum,

    /// A reader for reading the entry's data from the archive.
    pub blocks: Vec<BlockAddress>,
}

/// An entry in the archive.
#[derive(Debug)]
pub struct ArchiveEntry<'a> {
    /// The name of the entry.
    pub name: String,

    /// The metadata associated with this entry.
    pub metadata: HashMap<String, Vec<u8>>,

    /// The data associated with this entry.
    pub data: ArchiveData<'a>,
}

/// Data associated with an entry in the archive.
#[derive(Debug)]
pub struct ArchiveData<'a> {
    /// The size of the entry's data in bytes.
    size: u64,

    /// The 256-bit BLAKE2 checksum of the entry's data.
    checksum: Checksum,

    /// The archive that this reads data from.
    archive: &'a mut Archive,

    /// The header entry representing this data.
    entry: &'a mut HeaderData,
}

impl ArchiveData {
    /// Creates a new `ArchiveData` with no data.
    pub(super) fn new<'a>(archive: &'a mut Archive, entry: &'a mut HeaderData) -> Self <'a> {
        ArchiveData { size: 0, checksum: EMPTY_CHECKSUM, archive, entry }
    }

    /// The size of the entry's data in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The 256-bit BLAKE2 checksum of the entry's data.
    pub fn checksum(&self) -> Checksum {
        self.checksum
    }

    /// Returns a reader for reading the contents of the entry's data.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read(&mut self) -> Result<impl Read> {
        self.archive.read_entry_data(&self.entry.blocks)
    }

    /// Replaces the entry's data with the bytes from `source`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write(&mut self, source: &mut impl Read) -> Result<()> {
        let header_data = self.archive.write_entry_data(source)?;
        self.entry.size = header_data.size;
        self.entry.checksum = header_data.checksum;
        self.entry.blocks = header_data.blocks;

        Ok(())
    }
}
