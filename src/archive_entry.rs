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
use std::io::Read;
use std::path::PathBuf;

use chrono::{NaiveDateTime, Utc};
use relative_path::RelativePathBuf;

use crate::block::Checksum;

/// The contents of an archive entry.
pub enum EntryData {
    /// A regular file with opaque contents.
    File {
        /// The size of the file in bytes.
        size: u64,

        /// The 256-bit BLAKE2 checksum of the file.
        checksum: Checksum,

        /// The contents of the file.
        ///
        /// Bytes read from this reader are read directly from the archive.
        contents: Box<dyn Read>,
    },

    /// A directory.
    Directory,

    /// A symbolic link.
    Link {
        /// The path of the target of this symbolic link.
        ///
        /// This value is stored as a platform-dependent path. A symbolic link archived on one
        /// platform may not be able to be extracted on another platform with different path
        /// semantics.
        target: PathBuf
    },
}

/// Metadata about an archive entry.
pub struct EntryMetadata {
    /// The path of the file in the archive.
    pub path: RelativePathBuf,

    /// The time the file was last modified.
    pub modified_time: NaiveDateTime,

    /// The POSIX permissions bits of the file, or `None` if POSIX permissions are not applicable.
    pub permissions: Option<i32>,

    /// The file's extended attributes.
    pub attributes: HashMap<String, Vec<u8>>,
}

impl EntryMetadata {
    /// Create a new `EntryMetadata` with default values.
    pub fn new(path: RelativePathBuf) -> Self {
        EntryMetadata {
            path,
            modified_time: Utc::now().naive_utc(),
            permissions: None,
            attributes: HashMap::new(),
        }
    }
}
