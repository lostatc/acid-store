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

use relative_path::RelativePathBuf;
use serde::{Deserialize, Serialize};

use super::metadata::FileMetadata;
use crate::repo::file::special::SpecialType;

/// A type of file in a `FileRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum FileType<T> {
    /// A regular file.
    File,

    /// A directory.
    Directory,

    /// A special file.
    Special(T),
}

impl<T: SpecialType> From<T> for FileType<T> {
    fn from(file: T) -> Self {
        FileType::Special(file)
    }
}

/// An entry in a `FileRepository` which represents a regular file, directory, or special file.
///
/// An entry may or may not have metadata associated with it. When an entry is created by archiving
/// a file in the file system (`FileRepository::archive`), it will have the metadata of that file.
/// However, entries can also be created that have no metadata. This allows for extracting files to
/// the file system (`FileRepository::extract`) without copying any metadata.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Entry<T, M> {
    /// The type of file this entry represents.
    pub file_type: FileType<T>,

    /// The metadata for the file or `None` if the entry has no metadata.
    pub metadata: Option<M>,
}

impl<T: SpecialType, M: FileMetadata> Entry<T, M> {
    /// Create an `Entry` for a new regular file.
    ///
    /// The created entry will have no metadata.
    pub fn file() -> Self {
        Entry {
            file_type: FileType::File,
            metadata: None,
        }
    }

    /// Create an `Entry` for a new directory.
    ///
    /// The created entry will have no metadata.
    pub fn directory() -> Self {
        Entry {
            file_type: FileType::Directory,
            metadata: None,
        }
    }

    /// Create an `Entry` for a new special `file`.
    ///
    /// The created entry will have no metadata.
    pub fn special(file: T) -> Self {
        Entry {
            file_type: FileType::Special(file),
            metadata: None,
        }
    }

    /// Return whether this entry is a regular file.
    pub fn is_file(&self) -> bool {
        matches!(self.file_type, FileType::File)
    }

    /// Return whether this entry is a directory.
    pub fn is_directory(&self) -> bool {
        matches!(self.file_type, FileType::Directory)
    }

    /// Return whether this entry is a special file.
    pub fn is_special(&self) -> bool {
        matches!(self.file_type, FileType::Special(..))
    }
}

/// The key to use in the `ObjectRepository` which backs a `FileRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum EntryKey {
    /// The data for a file.
    Data(RelativePathBuf),

    /// The entry representing a file.
    Entry(RelativePathBuf),

    /// The repository version.
    RepositoryVersion,
}
