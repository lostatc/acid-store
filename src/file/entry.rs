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
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::SystemTime;

use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};

use crate::Object;

use super::serialization::SerializableRelativePathBuf;

/// A type of file which can be stored in a `FileArchive`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum EntryType {
    /// A regular file.
    File {
        /// A handle for accessing the contents of the file.
        data: Object,
    },

    /// A directory.
    Directory,

    /// A symbolic link.
    ///
    /// The link target is stored as a platform-dependent path, so a symlink archived on one system
    /// may be broken when extracted on another.
    Link {
        /// The file the symbolic link points to.
        target: PathBuf,
    },
}

/// Metadata about a file stored in a `FileArchive`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EntryMetadata {
    /// The time the file was last modified.
    pub modified_time: SystemTime,

    /// The POSIX permissions of the file, or `None` if POSIX permissions are not applicable.
    pub permissions: Option<u32>,

    /// The file's extended attributes.
    pub attributes: HashMap<OsString, Vec<u8>>,
}

/// A file stored in a `FileArchive`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// The metadata of the file this entry represents.
    pub(super) metadata: EntryMetadata,

    /// The type of file this entry represents.
    pub(super) entry_type: EntryType,
}

impl Entry {
    /// The metadata of the file this entry represents.
    pub fn metadata(&self) -> &EntryMetadata {
        &self.metadata
    }

    /// The type of file this entry represents.
    pub fn entry_type(&self) -> &EntryType {
        &self.entry_type
    }
}

/// A type which determines whether a key represents the data or metadata for an entry.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum KeyType {
    Data,
    Metadata,
}

/// A key to use in the `ObjectArchive` which backs the `FileArchive`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct EntryKey(
    #[serde(with = "SerializableRelativePathBuf")] pub RelativePathBuf,
    pub KeyType,
);
