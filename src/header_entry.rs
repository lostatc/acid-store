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
use std::path::PathBuf;

use chrono::NaiveDateTime;
use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};

use crate::block::{BlockAddress, Checksum};
use crate::serialization::{SerializableNaiveDateTime, SerializableRelativePathBuf};

/// A type of entry which can be stored in the archive header.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeaderEntryType {
    /// A regular file with opaque contents.
    File {
        /// The size of the file in bytes.
        size: u64,

        /// The BLAKE2 checksum of the file.
        checksum: Checksum,

        /// The addresses of blocks containing the data for this file.
        blocks: Vec<BlockAddress>,
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
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtendedAttribute {
    /// The name of the attribute.
    pub name: String,

    /// The value of the attribute.
    pub value: Vec<u8>,
}

/// File metadata which is stored in the archive header.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderEntry {
    /// The path of the file in the archive.
    #[serde(with = "SerializableRelativePathBuf")]
    pub path: RelativePathBuf,

    /// The time the file was last modified.
    #[serde(with = "SerializableNaiveDateTime")]
    pub modified_time: NaiveDateTime,

    /// The POSIX permissions bits of the file, or `None` if POSIX permissions are not applicable.
    pub permissions: Option<i32>,

    /// The file's extended attributes.
    pub attributes: Vec<ExtendedAttribute>,

    /// The type of file this entry represents.
    pub entry_type: HeaderEntryType,
}
