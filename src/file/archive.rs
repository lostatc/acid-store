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

use std::fs::{create_dir, create_dir_all, File, OpenOptions, read_link, symlink_metadata};
use std::io::{self, copy, ErrorKind, Read};
use std::path::Path;

use filetime::{FileTime, set_file_mtime};
use relative_path::RelativePath;
use walkdir::{Error, WalkDir};

use crate::{ArchiveConfig, Key, Object, ObjectArchive};

use super::entry::{Entry, EntryKey, EntryType, KeyType};
use super::platform::{extended_attrs, file_mode, set_extended_attrs, set_file_mode, soft_link};

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
    archive: ObjectArchive<EntryKey>,
}

impl FileArchive {
    /// Create a new archive at the given `path` with the given `config`.
    ///
    /// If encryption is enabled, an `encryption_key` must be provided. Otherwise, this argument
    /// can be `None`.
    ///
    /// # Errors
    /// - `ErrorKind::PermissionDenied`: The user lacks permission to create the archive file.
    /// - `ErrorKind::AlreadyExists`: A file already exists at `path`.
    /// - `ErrorKind::InvalidInput`: A key was required but not provided.
    /// - `ErrorKind::WouldBlock`: The archive is in use by another process.
    pub fn create(path: &Path, config: ArchiveConfig, key: Option<Key>) -> io::Result<Self> {
        Ok(FileArchive {
            archive: ObjectArchive::create(path, config, key)?,
        })
    }

    /// Opens the archive at the given `path`.
    ///
    /// If encryption is enabled, an `encryption_key` must be provided. Otherwise, this argument can
    /// be `None`.
    ///
    /// # Errors
    /// - `ErrorKind::NotFound`: The archive file does not exist.
    /// - `ErrorKind::PermissionDenied`: The user lacks permission to open the archive file.
    /// - `ErrorKind::InvalidInput`: A key was required but not provided.
    /// - `ErrorKind::InvalidData`: The header is corrupt.
    /// - `ErrorKind::InvalidData`: The wrong encryption key was provided.
    /// - `ErrorKind::WouldBlock`: The archive is in use by another process.
    pub fn open(path: &Path, key: Option<Key>) -> io::Result<Self> {
        Ok(FileArchive {
            archive: ObjectArchive::open(path, key)?,
        })
    }

    /// Returns the entry at `path` or `None` if there is none.
    pub fn entry(&self, path: &RelativePath) -> Option<Entry> {
        let object = self.archive.get(&EntryKey(path.to_owned(), KeyType::Metadata))?;
        Some(self.archive.deserialize(&object).expect("Could not deserialize entry."))
    }

    /// Returns an unordered list of archive entries which are children of `parent`.
    pub fn list(&self, parent: &RelativePath) -> Vec<&RelativePath> {
        self.archive
            .keys()
            .filter(|key| key.1 == KeyType::Metadata)
            .filter(|key| key.0.parent() == Some(parent))
            .map(|key| key.0.as_ref())
            .collect()
    }

    /// Returns an unordered list of archive entries which are descendants of `parent`.
    pub fn walk(&self, parent: &RelativePath) -> Vec<&RelativePath> {
        self.archive
            .keys()
            .filter(|key| key.1 == KeyType::Metadata)
            .filter(|key| key.0.starts_with(parent))
            .map(|key| key.0.as_ref())
            .collect()
    }

    /// Adds the given `entry` to the archive with the given `path`.
    ///
    /// If an entry with the given `path` already existed in the archive, it is replaced and the
    /// old entry is returned. Otherwise, `None` is returned.
    pub fn insert(&mut self, path: &RelativePath, entry: Entry) -> Option<Entry> {
        // Check if the entry exists.
        let old_entry = self.entry(path)?;

        // Write the metadata object.
        let metadata_object = self.archive.serialize(&entry).expect("Could not serialize entry.");
        self.archive.insert(EntryKey(path.to_owned(), KeyType::Metadata), metadata_object)?;

        // Write the data object.
        if let EntryType::File { data } = entry.entry_type {
            self.archive.insert(EntryKey(path.to_owned(), KeyType::Data), data);
        }

        Some(old_entry)
    }

    /// Delete the entry in the archive with the given `path`.
    ///
    /// This returns the removed entry or `None` if there was no entry at `path`.
    pub fn remove(&mut self, path: &RelativePath) -> Option<Entry> {
        let old_entry = match self.entry(path) {
            Some(value) => value,
            None => return None
        };

        self.archive.remove(&EntryKey(path.to_owned(), KeyType::Data));
        self.archive.remove(&EntryKey(path.to_owned(), KeyType::Metadata));

        Some(old_entry)
    }

    /// Writes the given `data` to the archive and returns a new object.
    ///
    /// The returned object can be used to manually construct an `Entry` that represents a
    /// regular file.
    pub fn write(&mut self, source: impl Read) -> io::Result<Object> {
        self.archive.write(source)
    }

    /// Returns a reader for reading the data associated with `object` from the archive.
    pub fn read<'a>(&'a self, object: &'a Object) -> impl Read + 'a {
        self.archive.read(object)
    }

    /// Copy a file from the file system into the archive.
    ///
    /// This creates an archive entry at `dest` from the file at `source`. This does not remove the
    /// `source` file from the file system.
    ///
    /// # Errors
    /// - `ErrorKind::NotFound`: The `source` file does not exist.
    /// - `ErrorKind::PermissionDenied`: The user lack permission to read the `source` file.
    /// - `ErrorKind::Other`: The file is not a regular file, symlink, or directory.
    pub fn archive(&mut self, source: &Path, dest: &RelativePath) -> io::Result<()> {
        let metadata = symlink_metadata(source)?;
        let file_type = metadata.file_type();

        // Get the file type.
        let entry_type = if file_type.is_file() {
            let object = self.write(&mut File::open(source)?)?;
            EntryType::File { data: object }
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
            ));
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
    /// - `ErrorKind::NotFound`: The `source` file does not exist.
    /// - `ErrorKind::PermissionDenied`: The user lack permission to read the `source` file.
    /// - `ErrorKind::Other`: A cycle of symbolic links was detected.
    pub fn archive_tree(&mut self, source: &Path, dest: &RelativePath) -> io::Result<()> {
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
    /// - `ErrorKind::NotFound`: The `source` entry does not exist.
    /// - `ErrorKind::PermissionDenied`: The user lack permission to create the `dest` file.
    /// - `ErrorKind::AlreadyExists`: A file already exists at `dest`.
    pub fn extract(&mut self, source: &RelativePath, dest: &Path) -> io::Result<()> {
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
                copy(&mut self.read(&data), &mut file)?;
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
    /// - `ErrorKind::PermissionDenied`: The user lack permission to create the `dest` file.
    /// - `ErrorKind::AlreadyExists`: A file already exists at `dest`.
    pub fn extract_tree(&mut self, source: &RelativePath, dest: &Path) -> io::Result<()> {
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

    /// Commit changes which have been made to the archive.
    ///
    /// See `Archive::commit` for details.
    pub fn commit(&mut self) -> io::Result<()> {
        self.archive.commit()
    }

    /// Copy the contents of this archive to a new archive file at `destination`.
    ///
    /// See `ObjectArchive::repack` for details.
    pub fn repack(&mut self, dest: &Path) -> io::Result<FileArchive> {
        Ok(FileArchive {
            archive: self.archive.repack(dest)?
        })
    }
}
