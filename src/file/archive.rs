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

use std::io::Read;
use std::path::Path;

use relative_path::RelativePath;

use crate::error::Result;
use crate::{Archive, DataHandle};

use super::entry::ArchiveEntry;

/// An archive for storing files.
///
/// This is a wrapper over `disk_archive::storage::Archive` which allows it to function as a file
/// archive like `zip` or `tar` rather than an object store. A `FileArchive` consists of
/// `ArchiveEntry` values which can represent a regular file, directory, or symbolic link.
///
/// This type provides a high-level API through the methods `archive`, `archive_tree`, `extract`,
/// and `extract_tree` for archiving and extracting files in the file system. It also provides
/// low-level access for manually creating, deleting, and querying entries in the archive.
pub struct FileArchive {
    archive: Archive,
}

impl FileArchive {
    /// Opens the archive at the given `path`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Deserialize`: An error occurred deserializing the header.
    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }

    /// Creates and opens a new archive at the given `path`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    /// - `Error::Deserialize`: An error occurred deserializing the header.
    pub fn create(path: &Path) -> Result<Self> {
        unimplemented!()
    }

    /// Returns the entry at `path`.
    pub fn entry(path: &RelativePath) -> &ArchiveEntry {
        unimplemented!()
    }

    /// Returns a list of archive entries which are children of `parent`.
    pub fn list(parent: &RelativePath) -> Vec<&ArchiveEntry> {
        unimplemented!()
    }

    /// Returns a list of archive entries which are descendants of `parent`.
    pub fn walk(parent: &RelativePath) -> Vec<&ArchiveEntry> {
        unimplemented!()
    }

    /// Adds the given `entry` to the archive with the given `path`.
    ///
    /// If an entry with the given `path` already existed in the archive, it is replaced and the
    /// old entry is returned. Otherwise, `None` is returned.
    pub fn add(&mut self, path: &RelativePath, entry: ArchiveEntry) -> Option<ArchiveEntry> {
        unimplemented!()
    }

    /// Delete the entry in the archive with the given `path`.
    ///
    /// This returns the removed entry or `None` if there was no entry at `path`.
    pub fn remove(path: &RelativePath) -> Option<ArchiveEntry> {
        unimplemented!()
    }

    /// Returns a reader for reading the data associated with the given `handle`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read(&self, handle: &DataHandle) -> Result<impl Read> {
        unimplemented!()
    }

    /// Writes the data from `source` to the archive and returns a handle to it.
    ///
    /// The returned handle can be used to manually construct an `ArchiveEntry` that represents a
    /// regular file.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write(&mut self, source: &mut impl Read) -> Result<DataHandle> {
        unimplemented!()
    }

    /// Create an archive entry at `dest` from the file at `source`.
    ///
    /// This does not remove the `source` file from the file system.
    pub fn archive(&mut self, source: &Path, dest: &RelativePath) -> Result<()> {
        unimplemented!()
    }

    /// Create a tree of archive entries at `dest` from the directory tree at `source`.
    ///
    /// This does not remove the `source` directory or its descendants from the file system.
    pub fn archive_tree(&mut self, source: &Path, dest: &RelativePath) -> Result<()> {
        unimplemented!()
    }

    /// Create a file at `dest` from the archive entry at `source`.
    ///
    /// This does not remove the `source` entry from the archive.
    pub fn extract(&mut self, source: &RelativePath, dest: &Path) -> Result<()> {
        unimplemented!()
    }

    /// Create a directory tree at `dest` from the tree of archive entries at `source`.
    ///
    /// This does not remove the `source` entry or its descendants from the archive.
    pub fn extract_tree(&mut self, source: &RelativePath, dest: &Path) -> Result<()> {
        unimplemented!()
    }

    /// Commits all changes that have been made to the archive.
    ///
    /// See `disk_archive::storage::Archive::commit` for details.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn commit(&mut self) -> Result<()> {
        unimplemented!()
    }

    /// Creates a copy of this archive which is compacted to reduce its size.
    ///
    /// See `disk_archive::storage::Archive::compacted` for details.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn compacted(&mut self, dest: &Path) -> Result<FileArchive> {
        unimplemented!()
    }
}
