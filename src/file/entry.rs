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

use super::serialization::SerializableRelativePathBuf;

/// A type of file which can be stored in a `FileArchive`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum EntryType {
    /// A regular file.
    File,

    /// A directory.
    Directory,

    /// A symbolic link.
    Link {
        /// The path this symbolic link points to.
        ///
        /// This is stored as a platform-dependent path, meaning that a symlink archived on one
        /// platform may not be able to be extracted on another.
        target: PathBuf,
    },
}

/// Metadata about a file stored in a `FileArchive`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// The time the file was last modified.
    pub modified: SystemTime,

    /// The POSIX permissions of the file, or `None` if POSIX permissions are not applicable.
    pub permissions: Option<u32>,

    /// The file's extended attributes.
    pub attributes: HashMap<OsString, Vec<u8>>,

    /// The type of file this entry represents.
    pub entry_type: EntryType,
}

impl Entry {
    /// Create a new file entry with default values.
    pub fn file() -> Self {
        Self {
            modified: SystemTime::now(),
            permissions: None,
            attributes: HashMap::new(),
            entry_type: EntryType::File,
        }
    }

    /// Create a new directory entry with default values.
    pub fn directory() -> Self {
        Self {
            modified: SystemTime::now(),
            permissions: None,
            attributes: HashMap::new(),
            entry_type: EntryType::Directory,
        }
    }

    /// Create a new symbolic link entry with default values.
    pub fn link(target: PathBuf) -> Self {
        Self {
            modified: SystemTime::now(),
            permissions: None,
            attributes: HashMap::new(),
            entry_type: EntryType::Link { target },
        }
    }
}

// TODO: Replace with a single enum.

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
