/*
 * Copyright 2019-2021 Wren Powell
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

use serde::{Deserialize, Serialize};

use crate::repo::state::ObjectKey;

use super::metadata::FileMetadata;
use super::special::SpecialType;

/// A type of file in a `FileRepo`.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType<S> {
    /// A regular file.
    File,

    /// A directory.
    Directory,

    /// A special file.
    Special(S),
}

impl<S: SpecialType> From<S> for FileType<S> {
    fn from(file: S) -> Self {
        FileType::Special(file)
    }
}

/// An entry in a [`FileRepo`] which represents a regular file, directory, or special file.
///
/// An entry may or may not have metadata associated with it. When an entry is created by archiving
/// a file in the file system ([`FileRepo::archive`]), it will have the metadata of that file.
/// However, entries can also be created that have no metadata. This allows for extracting files to
/// the file system ([`FileRepo::extract`]) without copying any metadata.
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`FileRepo::archive`]: crate::repo::file::FileRepo::archive
/// [`FileRepo::extract`]: crate::repo::file::FileRepo::extract
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry<S, M> {
    /// The type of file this entry represents.
    pub file_type: FileType<S>,

    /// The metadata for the file or `None` if the entry has no metadata.
    pub metadata: Option<M>,
}

impl<S: SpecialType, M: FileMetadata> Entry<S, M> {
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
    pub fn special(file: S) -> Self {
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

/// A type of entry handle.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum EntryType {
    File(ObjectKey),
    Directory,
    Special,
}

/// A handle for accessing the data associated with each entry.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EntryHandle {
    pub entry: ObjectKey,
    pub entry_type: EntryType,
}
