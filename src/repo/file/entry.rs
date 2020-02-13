/*
 * Copyright 2019-2020 Wren Powell
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

use serde::{Deserialize, Serialize};

use super::metadata::FileMetadata;

/// A type of file in a `FileRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum FileType {
    /// A regular file.
    File,

    /// A directory.
    Directory,
}

/// An entry in a `FileRepository` which represents a file or directory.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Entry<M> {
    /// The type of file this entry represents.
    pub file_type: FileType,

    /// The metadata for the file.
    pub metadata: M,
}

impl<M: FileMetadata> Entry<M> {
    /// Create an `Entry` for a new regular file.
    pub fn file() -> Self {
        Entry {
            file_type: FileType::File,
            metadata: M::default(),
        }
    }

    /// Create an `Entry` for a new directory.
    pub fn directory() -> Self {
        Entry {
            file_type: FileType::Directory,
            metadata: M::default(),
        }
    }

    /// Return whether this entry is a regular file.
    pub fn is_file(&self) -> bool {
        self.file_type == FileType::File
    }

    /// Return whether this entry is a directory.
    pub fn is_directory(&self) -> bool {
        self.file_type == FileType::Directory
    }
}

/// The key to use in the `ObjectRepository` which backs a `FileRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum EntryKey {
    /// The data for a file.
    Data(PathBuf),

    /// The entry representing a file.
    Entry(PathBuf),

    /// The repository version.
    Version,
}
