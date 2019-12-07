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

use std::fs::{create_dir, create_dir_all, read_link, symlink_metadata, File, OpenOptions};
use std::io::{self, copy, ErrorKind, Read};
use std::path::Path;

use filetime::{set_file_mtime, FileTime};
use relative_path::RelativePath;
use rmp_serde::{decode, encode};
use walkdir::WalkDir;

use crate::{ArchiveConfig, DataHandle, EntryType, Object, ObjectArchive, Result};

use super::entry::Entry;
use super::platform::{extended_attrs, file_mode, set_extended_attrs, set_file_mode, soft_link};

impl Object {
    /// Convert this object into an entry.
    fn to_entry(&self) -> Entry {
        decode::from_read_ref(&self.metadata).expect("Could not deserialize file metadata.")
    }
}

impl Entry {
    /// Convert this entry into an object.
    fn to_object(&self) -> Object {
        let mut object = Object::new();

        // TODO: Avoid storing the data handle in the object twice.
        object.metadata = encode::to_vec(&self).expect("Could not serialize file metadata.");
        if let EntryType::File { data } = &self.entry_type {
            object.data = data.clone();
        }

        object
    }
}

/// An archive for storing files.
///
/// This is a wrapper over `ObjectArchive` which allows it to function as a file archive like `zip`
/// or `tar` rather than an object store. A `FileArchive` consists of `Entry` values which
/// can represent a regular file, directory, or symbolic link.
///
/// This type provides a high-level API through the methods `archive`, `archive_tree`, `extract`,
/// and `extract_tree` for archiving and extracting files in the file system. It also provides
/// low-level access for manually creating, deleting, and querying entries in the archive.
///
/// While files in the file system are identified by their `Path`, entries in the archive are
/// identified by a `RelativePath`. A `RelativePath` is a platform-independent path representation
/// that allows entries archived on one system to be extracted on another.
pub struct FileArchive {
    archive: ObjectArchive,
}

impl FileArchive {
    /// Opens the archive at the given `path`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `NotFound`: The archive file does not exist.
    ///     - `PermissionDenied`: The user lack permission to open the archive file.
    /// - `Error::Deserialize`: The file is not a valid archive file.
    pub fn open(path: &Path) -> Result<Self> {
        Ok(FileArchive {
            archive: ObjectArchive::open(path)?,
        })
    }

    /// Creates and opens a new archive at the given `path` with the given `config`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `PermissionDenied`: The user lack permission to create the archive file.
    ///     - `AlreadyExists`: A file already exists at `path`.
    pub fn create(path: &Path, config: ArchiveConfig) -> Result<Self> {
        Ok(FileArchive {
            archive: ObjectArchive::create(path, config)?,
        })
    }

    /// Returns the entry at `path` or `None` if there is none.
    pub fn entry(&self, path: &RelativePath) -> Option<Entry> {
        Some(self.archive.get(path.as_str())?.to_entry())
    }

    /// Returns an unordered list of archive entries which are children of `parent`.
    pub fn list(&self, parent: &RelativePath) -> Vec<&RelativePath> {
        self.archive
            .names()
            .map(|name| RelativePath::new(name))
            .filter(|path| path.parent() == Some(parent))
            .collect()
    }

    /// Returns an unordered list of archive entries which are descendants of `parent`.
    pub fn walk(&self, parent: &RelativePath) -> Vec<&RelativePath> {
        self.archive
            .names()
            .map(|name| RelativePath::new(name))
            .filter(|path| path.starts_with(parent))
            .collect()
    }

    /// Adds the given `entry` to the archive with the given `path`.
    ///
    /// If an entry with the given `path` already existed in the archive, it is replaced and the
    /// old entry is returned. Otherwise, `None` is returned.
    pub fn insert(&mut self, path: &RelativePath, entry: Entry) -> Option<Entry> {
        Some(
            self.archive
                .insert(path.as_str(), entry.to_object())?
                .to_entry(),
        )
    }

    /// Delete the entry in the archive with the given `path`.
    ///
    /// This returns the removed entry or `None` if there was no entry at `path`.
    pub fn remove(&mut self, path: &RelativePath) -> Option<Entry> {
        Some(self.archive.remove(path.as_str())?.to_entry())
    }

    /// Returns a reader for reading the data associated with the given `handle`.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn read(&self, handle: &DataHandle) -> Result<impl Read> {
        self.archive.read(handle)
    }

    /// Writes the data from `source` to the archive and returns a handle to it.
    ///
    /// The returned handle can be used to manually construct an `Entry` that represents a
    /// regular file.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn write(&mut self, source: impl Read) -> Result<DataHandle> {
        self.archive.write(source)
    }

    /// Copy a file from the file system into the archive.
    ///
    /// This creates an archive entry at `dest` from the file at `source`. This does not remove the
    /// `source` file from the file system.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `NotFound`: The `source` file does not exist.
    ///     - `PermissionDenied`: The user lack permission to read the `source` file.
    pub fn archive(&mut self, source: &Path, dest: &RelativePath) -> Result<()> {
        let metadata = symlink_metadata(source)?;
        let file_type = metadata.file_type();

        // Get the file type.
        let entry_type = if file_type.is_file() {
            let handle = self.write(&mut File::open(source)?)?;
            EntryType::File { data: handle }
        } else if file_type.is_dir() {
            EntryType::Directory
        } else if file_type.is_symlink() {
            EntryType::Link {
                target: read_link(source)?,
            }
        } else {
            return Err(io::Error::new(
                ErrorKind::Other,
                "This file is not a regular file, symlink or directory.",
            )
            .into());
        };

        // Create an entry.
        let entry = Entry {
            modified_time: metadata.modified()?,
            permissions: file_mode(&metadata),
            attributes: extended_attrs(&source)?,
            entry_type,
        };

        self.insert(dest, entry);

        Ok(())
    }

    /// Copy a directory tree from the file system into the archive.
    ///
    /// This creates a tree of archive entries at `dest` from the directory tree at `source`. This
    /// does not remove the `source` directory or its descendants from the file system.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `NotFound`: The `source` file does not exist.
    ///     - `PermissionDenied`: The user lack permission to read the `source` file.
    /// - `Error::Walk` There was an error walking the directory tree.
    pub fn archive_tree(&mut self, source: &Path, dest: &RelativePath) -> Result<()> {
        for result in WalkDir::new(source) {
            let dir_entry = result?;
            let relative_path = dir_entry.path().strip_prefix(source).unwrap();
            let entry_path = dest.join(RelativePath::from_path(relative_path).unwrap());
            self.archive(dir_entry.path(), entry_path.as_relative_path())?;
        }

        Ok(())
    }

    /// Copy a file from the archive into the file system.
    ///
    /// This creates a file at `dest` from the archive entry at `source`. This does not remove the
    /// `source` entry from the archive.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `PermissionDenied`: The user lack permission to create the `dest` file.
    ///     - `AlreadyExists`: A file already exists at `dest`.
    pub fn extract(&mut self, source: &RelativePath, dest: &Path) -> Result<()> {
        let entry = match self.entry(source) {
            Some(value) => value,
            None => {
                return Err(io::Error::new(ErrorKind::NotFound, "There is no such entry.").into())
            }
        };

        // Create any necessary parent directories.
        if let Some(parent) = dest.parent() {
            create_dir_all(parent)?
        }

        // Create the file, directory, or symlink.
        match entry.entry_type {
            EntryType::File { data } => {
                let mut file = OpenOptions::new().write(true).create_new(true).open(dest)?;
                copy(&mut self.read(&data)?, &mut file)?;
            }
            EntryType::Directory => {
                create_dir(dest)?;
            }
            EntryType::Link { target } => {
                soft_link(dest, &target)?;
            }
        }

        // Set the file metadata.
        set_file_mtime(dest, FileTime::from_system_time(entry.modified_time))?;
        if let Some(mode) = entry.permissions {
            set_file_mode(dest, mode)?;
        }
        set_extended_attrs(dest, entry.attributes)?;

        Ok(())
    }

    /// Copy a directory tree from the archive into the file system.
    ///
    /// This creates a directory tree at `dest` from the tree of archive entries at `source`. This
    /// does not remove the `source` entry or its descendants from the archive.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `PermissionDenied`: The user lack permission to create the `dest` file.
    ///     - `AlreadyExists`: A file already exists at `dest`.
    pub fn extract_tree(&mut self, source: &RelativePath, dest: &Path) -> Result<()> {
        // We must convert to owned paths because we'll need a mutable reference to `self` later.
        let mut descendants = self
            .walk(source)
            .into_iter()
            .map(|path| path.to_relative_path_buf())
            .collect::<Vec<_>>();

        // Sort the descendants by depth.
        descendants.sort_by_key(|path| path.components().count());

        for entry_path in descendants {
            let file_path = entry_path.to_path(dest);
            self.extract(entry_path.as_relative_path(), file_path.as_path())?;
        }

        Ok(())
    }

    /// Commits all changes that have been made to the archive.
    ///
    /// See `Archive::commit` for details.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn commit(&mut self) -> Result<()> {
        self.archive.commit()
    }

    /// Creates a copy of this archive which is compacted to reduce its size.
    ///
    /// See `Archive::compacted` for details.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    ///     - `PermissionDenied`: The user lack permission to create the new archive.
    ///     - `AlreadyExists`: A file already exists at `dest`.
    pub fn compacted(&mut self, dest: &Path) -> Result<FileArchive> {
        Ok(FileArchive {
            archive: self.archive.compacted(dest)?,
        })
    }
}
