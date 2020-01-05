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

use std::collections::HashSet;
use std::fs::{create_dir, create_dir_all, File, OpenOptions, read_link, symlink_metadata};
use std::io::{self, copy, ErrorKind, Read};
use std::path::{Path, PathBuf};

use filetime::{FileTime, set_file_mtime};
use relative_path::RelativePath;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::{DataStore, LockStrategy, ObjectHandle, ObjectRepository, RepositoryConfig};

use super::entry::{Entry, EntryKey, EntryMetadata, EntryType, KeyType};
use super::platform::{extended_attrs, file_mode, set_extended_attrs, set_file_mode, soft_link};

/// A persistent file store.
///
/// This is a wrapper around `ObjectRepository` which allows it to function as a file archive like
/// ZIP or TAR rather than an object store. A `FileArchive` consists of `Entry` values which can
/// represent a regular file, directory, or symbolic link.
///
/// This type provides a high-level API through the methods `archive`, `archive_tree`, `extract`,
/// and `extract_tree` for archiving and extracting files in the file system. It also allows for
/// manually adding entries through the methods `add_file`, `add_directory`, and `add_link`.
///
/// While files in the file system are identified by their `Path`, entries in the archive are
/// identified by a `RelativePath`. A `RelativePath` is a platform-independent path representation
/// that allows entries archived on one system to be extracted on another.
///
/// Like `ObjectRepository`, changes made to the repository are not persisted to disk until `commit`
/// is called. For details about deduplication, compression, encryption, and locking, see
/// `ObjectRepository`.
pub struct FileRepository<S: DataStore> {
    repository: ObjectRepository<EntryKey, S>,
}

impl<S: DataStore> FileRepository<S> {
    /// Create a new repository backed by the given data `store`.
    ///
    /// See `ObjectRepository::create` for details.
    pub fn create(
        store: S,
        config: RepositoryConfig,
        password: Option<&[u8]>,
        strategy: LockStrategy,
    ) -> io::Result<Self> {
        Ok(FileRepository {
            repository: ObjectRepository::create(store, config, password, strategy)?,
        })
    }

    /// Open the repository in the given data `store`.
    ///
    /// See `ObjectRepository::open` for details.
    pub fn open(store: S, password: Option<&[u8]>, strategy: LockStrategy) -> io::Result<Self> {
        Ok(FileRepository {
            repository: ObjectRepository::open(store, password, strategy)?,
        })
    }

    /// Return the entry at `path` or `None` if there is none.
    pub fn entry(&self, path: &RelativePath) -> Option<Entry> {
        let object = self
            .repository
            .get(&EntryKey(path.to_owned(), KeyType::Metadata))?;

        Some(
            self.repository
                .deserialize(&object)
                .expect("Could not deserialize entry."),
        )
    }

    /// Remove and return the entry with the given `path` from the repository.
    ///
    /// This returns `None` if there is no entry with the given `path`.
    ///
    /// The space used by the given entry isn't freed and made available for new entries until
    /// `commit` is called.
    pub fn remove(&mut self, path: &RelativePath) -> Option<Entry> {
        let old_entry = self.entry(path)?;

        self.repository
            .remove(&EntryKey(path.to_owned(), KeyType::Data));
        self.repository
            .remove(&EntryKey(path.to_owned(), KeyType::Metadata));

        Some(old_entry)
    }

    /// Returns an iterator of paths which are children of `parent`.
    pub fn list<'a>(
        &'a self,
        parent: &'a RelativePath,
    ) -> impl Iterator<Item = &RelativePath> + 'a {
        self.repository
            .keys()
            .filter(|key| key.1 == KeyType::Metadata)
            .filter(move |key| key.0.parent() == Some(parent))
            .map(|key| key.0.as_ref())
    }

    /// Returns an iterator of paths which are descendants of `parent`.
    pub fn walk<'a>(
        &'a self,
        parent: &'a RelativePath,
    ) -> impl Iterator<Item = &RelativePath> + 'a {
        self.repository
            .keys()
            .filter(|key| key.1 == KeyType::Metadata)
            .filter(move |key| key.0.starts_with(parent))
            .map(|key| key.0.as_ref())
    }

    /// Copy a file from the file system into the repository.
    ///
    /// This creates a repository entry at `dest` from the file at `source` and returns the entry.
    /// This does not remove the `source` file from the file system.
    ///
    /// # Errors
    /// - `ErrorKind::NotFound`: The `source` file does not exist.
    /// - `ErrorKind::PermissionDenied`: The user lack permission to read the `source` file.
    /// - `ErrorKind::InvalidInput`: The file is not a regular file, symlink, or directory.
    pub fn archive(&mut self, source: &Path, dest: &RelativePath) -> io::Result<Entry> {
        let file_metadata = symlink_metadata(source)?;
        let file_type = file_metadata.file_type();

        // Get the file metadata.
        let metadata = EntryMetadata {
            modified_time: file_metadata.modified()?,
            permissions: file_mode(&file_metadata),
            attributes: extended_attrs(&source)?,
        };

        // Add the entry.
        if file_type.is_file() {
            self.add_file(dest, metadata, &mut File::open(source)?)
        } else if file_type.is_dir() {
            self.add_directory(dest, metadata)
        } else if file_type.is_symlink() {
            let target = read_link(source)?;
            self.add_link(dest, metadata, target)
        } else {
            Err(io::Error::new(
                ErrorKind::InvalidInput,
                "This file is not a regular file, symlink or directory.",
            ))
        }
    }

    /// Copy a directory tree from the file system into the repository.
    ///
    /// This creates a tree of repository entries at `dest` from the directory tree at `source`.
    /// This does not remove the `source` directory or its descendants from the file system.
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

    /// Copy a file from the repository into the file system.
    ///
    /// This creates a file at `dest` from the archive entry at `source`. This does not remove the
    /// `source` entry from the repository.
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
        set_file_mtime(
            dest,
            FileTime::from_system_time(entry.metadata.modified_time),
        )?;
        if let Some(mode) = entry.metadata.permissions {
            set_file_mode(dest, mode)?;
        }
        set_extended_attrs(dest, entry.metadata.attributes)?;

        Ok(())
    }

    /// Copy a directory tree from the repository into the file system.
    ///
    /// This creates a directory tree at `dest` from the tree of repository entries at `source`.
    /// This does not remove the `source` entry or its descendants from the repository.
    ///
    /// # Errors
    /// - `ErrorKind::PermissionDenied`: The user lack permission to create the `dest` file.
    /// - `ErrorKind::AlreadyExists`: A file already exists at `dest`.
    pub fn extract_tree(&mut self, source: &RelativePath, dest: &Path) -> io::Result<()> {
        // We must convert to owned paths because we'll need a mutable reference to `self` later.
        let mut descendants = self
            .walk(source)
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

    /// Write the given entry to the repository at the given `path`.
    fn add_entry(&mut self, path: &RelativePath, entry: &Entry) {
        self.repository
            .serialize(EntryKey(path.to_owned(), KeyType::Metadata), &entry)
            .expect("Could not serialize entry.");
    }

    /// Add a regular file with the given `path`, `metadata`, and `data` to the repository.
    pub fn add_file(
        &mut self,
        path: &RelativePath,
        metadata: EntryMetadata,
        data: impl Read,
    ) -> io::Result<Entry> {
        let data_key = EntryKey(path.to_owned(), KeyType::Data);
        let object = self.repository.write(data_key, data)?;
        let entry_type = EntryType::File {
            data: object.clone(),
        };
        let entry = Entry {
            metadata,
            entry_type,
        };

        self.add_entry(&path, &entry);

        Ok(entry)
    }

    /// Add a directory with the given `path` and `metadata` to the repository.
    pub fn add_directory(
        &mut self,
        path: &RelativePath,
        metadata: EntryMetadata,
    ) -> io::Result<Entry> {
        let entry_type = EntryType::Directory;
        let entry = Entry {
            metadata,
            entry_type,
        };

        self.add_entry(&path, &entry);

        Ok(entry)
    }

    /// Add a symbolic link with the given `path`, `metadata`, and `target` to the repository.
    pub fn add_link(
        &mut self,
        path: &RelativePath,
        metadata: EntryMetadata,
        target: PathBuf,
    ) -> io::Result<Entry> {
        let entry_type = EntryType::Link { target };
        let entry = Entry {
            metadata,
            entry_type,
        };

        self.add_entry(&path, &entry);

        Ok(entry)
    }

    /// Return a reader for reading the data associated with `object` from the repository.
    pub fn read<'a>(&'a self, object: &'a ObjectHandle) -> impl Read + 'a {
        self.repository.read(object)
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> io::Result<()> {
        self.repository.commit()
    }

    /// Verify the integrity of the data associated with `object`.
    ///
    /// See `ObjectRepository::verify_object` for details.
    pub fn verify_object(&self, object: &ObjectHandle) -> io::Result<bool> {
        self.repository.verify_object(object)
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// See `ObjectRepository::verify_repository` for details.
    pub fn verify_repository(&self) -> io::Result<HashSet<&RelativePath>> {
        self.repository
            .verify_repository()
            .map(|set| set.iter().map(|key| key.0.as_ref()).collect())
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepository::change_password` for details.
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repository.change_password(new_password);
    }

    /// Return the UUID of the repository.
    pub fn uuid(&self) -> Uuid {
        self.repository.uuid()
    }

    /// Return the UUID of the repository at `store` without opening it.
    ///
    /// See `ObjectRepository::peek_uuid` for details.
    pub fn peek_uuid(store: S) -> io::Result<Uuid> {
        ObjectRepository::<EntryKey, S>::peek_uuid(store)
    }
}
