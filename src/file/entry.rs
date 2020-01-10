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

/// A type of file in a `FileRepository`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum FileType {
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

/// Metadata about a file stored in a `FileRepository`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// The time the file was last modified.
    pub modified: SystemTime,

    /// The POSIX permissions of the file, or `None` if POSIX permissions are not applicable.
    pub permissions: Option<u32>,

    /// The file's extended attributes.
    pub attributes: HashMap<OsString, Vec<u8>>,

    /// The type of the file.
    pub file_type: FileType,
}

impl FileMetadata {
    /// Create metadata for a new file.
    pub fn file() -> Self {
        Self {
            modified: SystemTime::now(),
            permissions: None,
            attributes: HashMap::new(),
            file_type: FileType::File,
        }
    }

    /// Create metadata for a new directory.
    pub fn directory() -> Self {
        Self {
            modified: SystemTime::now(),
            permissions: None,
            attributes: HashMap::new(),
            file_type: FileType::Directory,
        }
    }

    /// Create metadata for a new symbolic link.
    pub fn link(target: PathBuf) -> Self {
        Self {
            modified: SystemTime::now(),
            permissions: None,
            attributes: HashMap::new(),
            file_type: FileType::Link { target },
        }
    }

    /// Return whether the file is a regular file.
    pub fn is_file(&self) -> bool {
        self.file_type == FileType::File
    }

    /// Return whether the file is a directory.
    pub fn is_directory(&self) -> bool {
        self.file_type == FileType::Directory
    }

    /// Return whether the file is a symbolic link.
    pub fn is_link(&self) -> bool {
        match self.file_type {
            FileType::Link { .. } => true,
            _ => false,
        }
    }
}

/// The key to use in the `ObjectRepository` which backs a `FileRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Entry {
    /// The data for a file.
    Data(#[serde(with = "SerializableRelativePathBuf")] RelativePathBuf),

    /// The metadata for a file.
    Metadata(#[serde(with = "SerializableRelativePathBuf")] RelativePathBuf),
}
