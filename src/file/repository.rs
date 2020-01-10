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

use std::cmp::Reverse;
use std::collections::HashSet;
use std::fs::{create_dir, create_dir_all, File, OpenOptions, read_link, symlink_metadata};
use std::io::{self, copy, Write};
use std::path::Path;

use filetime::{FileTime, set_file_mtime};
use relative_path::RelativePath;
use rmp_serde::{from_read, to_vec};
use walkdir::WalkDir;

use crate::{DataStore, LockStrategy, Object, ObjectRepository, RepositoryConfig, RepositoryInfo};

use super::entry::{Entry, EntryKey, EntryType, KeyType};
use super::platform::{extended_attrs, file_mode, set_extended_attrs, set_file_mode, soft_link};

/// A persistent file store.
///
/// This is a wrapper around `ObjectRepository` which allows it to function as a file archive like
/// ZIP or TAR. A `FileArchive` consists of `Entry` values which can represent either a regular
/// file, directory, or symbolic link.
///
/// This repository provides a high-level API for copying files between the file system and
/// repository through the methods `archive`, `archive_tree`, `extract`, and `extract_tree`. It is
/// also possible to manually add, remove, query, and modify entries.
///
/// While files in the file system are identified by their `Path`, entries in the archive are
/// identified by a `RelativePath`. A `RelativePath` is a platform-independent path representation
/// that allows entries archived on one system to be extracted on another. Each `RelativePath` is
/// relative to the root of the archive.
///
/// `FileRepository` is more like a file archive than a file system. An entry's permissions
/// (`Entry::permissions`) do not affect whether the entry can be read from or written to within the
/// repository. An entry's modification time (`Entry::modified`) is not updated when an entry is
/// modified in the repository. This metadata is only stored in the repository so that it can be
/// restored when the entry is extracted to the file system.
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
    /// See `ObjectRepository::create_repo` for details.
    pub fn create_repo(
        store: S,
        config: RepositoryConfig,
        password: Option<&[u8]>,
    ) -> crate::Result<Self> {
        Ok(FileRepository {
            repository: ObjectRepository::create_repo(store, config, password)?,
        })
    }

    /// Open the existing repository in the given data `store`.
    ///
    /// See `ObjectRepository::open_repo` for details.
    pub fn open_repo(store: S, password: Option<&[u8]>, strategy: LockStrategy) -> crate::Result<Self> {
        Ok(FileRepository {
            repository: ObjectRepository::open_repo(store, password, strategy)?,
        })
    }

    /// Return whether there is an entry at `path`.
    pub fn exists(&self, path: &RelativePath) -> bool {
        self.repository.contains(&EntryKey(path.to_owned(), KeyType::Metadata))
    }

    /// Add a new `entry` to the repository at the given `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `path` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `path`.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create(&mut self, path: &RelativePath, entry: &Entry) -> crate::Result<()> {
        if self.exists(path) {
            return Err(crate::Error::AlreadyExists);
        }

        let data_key = EntryKey(path.to_owned(), KeyType::Data);
        let metadata_key = EntryKey(path.to_owned(), KeyType::Metadata);

        // Check if the parent directory exists.
        if let Some(parent) = path.parent() {
            match self.entry(parent) {
                Some(entry) => if entry.entry_type != EntryType::Directory {
                    return Err(crate::Error::InvalidPath);
                },
                None => return Err(crate::Error::InvalidPath),
            }
        }

        // Remove any existing data object and add a new one if this is a file entry.
        if let EntryType::File = entry.entry_type {
            self.repository.insert(data_key);
        } else {
            self.repository.remove(&data_key);
        }

        // Write the metadata for the entry.
        let mut object = self.repository.insert(metadata_key);
        Ok(object.write_all(
            to_vec(&entry)
                .expect("Could not serialize entry.")
                .as_slice()
        )?)
    }

    /// Add a new `entry` to the repository at the given `path`.
    ///
    /// This also creates any missing parent directories.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: There is already an entry at `path`.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create_parents(&mut self, path: &RelativePath, entry: &Entry) -> crate::Result<()> {
        let mut ancestor = path.parent();
        while let Some(directory) = ancestor {
            if self.exists(directory) { break; }
            self.create(directory, &Entry::directory())?;
            ancestor = directory.parent();
        }

        self.create(path, entry)
    }

    /// Remove and return the entry with the given `path` from the repository.
    ///
    /// The space used by the given entry isn't freed and made available for new entries until
    /// `commit` is called.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotEmpty`: The entry is a directory which is not empty.
    pub fn remove(&mut self, path: &RelativePath) -> crate::Result<Entry> {
        let old_entry = match self.entry(path) {
            Some(entry) => entry,
            None => return Err(crate::Error::NotFound),
        };

        if let Ok(children) = self.list(path) {
            if !children.is_empty() {
                return Err(crate::Error::NotEmpty);
            }
        }

        self.repository
            .remove(&EntryKey(path.to_owned(), KeyType::Data));
        self.repository
            .remove(&EntryKey(path.to_owned(), KeyType::Metadata));

        Ok(old_entry)
    }

    /// Remove and return the entry with the given `path` and its descendants from the repository.
    ///
    /// The space used by the given entry isn't freed and made available for new entries until
    /// `commit` is called.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    pub fn remove_tree(&mut self, path: &RelativePath) -> crate::Result<Entry> {
        match self.walk(path) {
            Ok(descendants) => {
                // We must convert to owned paths so we're not borrowing `self`.
                let mut owned_descendants = descendants
                    .into_iter()
                    .map(|path| path.to_relative_path_buf())
                    .collect::<Vec<_>>();

                // Sort paths in reverse order by depth.
                owned_descendants.sort_by_key(|path| Reverse(path.iter().count()));

                // Extract the descendants.
                for descendant in owned_descendants {
                    self.remove(descendant.as_relative_path())?;
                }

                // Extract the root directory.
                self.remove(path)
            },
            Err(crate::Error::NotDirectory) => self.remove(path),
            Err(error) => return Err(error),
        }
    }

    /// Return the entry at `path` or `None` if there is none.
    pub fn entry(&mut self, path: &RelativePath) -> Option<Entry> {
        let object = self
            .repository
            .get(&EntryKey(path.to_owned(), KeyType::Metadata))?;

        Some(from_read(object).expect("Could not deserialize entry."))
    }

    /// Return an `Object` for reading and writing the contents of the file entry at `path`.
    ///
    /// Writing to the returned object does not update the `Entry::modified` value for the entry.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotFile`: The entry is not a regular file.
    pub fn open(&mut self, path: &RelativePath) -> crate::Result<Object<EntryKey, S>> {
        let entry = self.entry(path).ok_or(crate::Error::NotFound)?;
        if entry.entry_type != EntryType::File {
            return Err(crate::Error::NotFile);
        }
        let object = self.repository.get(&EntryKey(path.to_owned(), KeyType::Data))
            .expect("There is no object associated with this file entry.");
        Ok(object)
    }

    /// Copy the entry at `source` to `dest`.
    ///
    /// If `source` is a directory entry, its descendants are not copied.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    pub fn copy(&mut self, source: &RelativePath, dest: &RelativePath) -> crate::Result<()> {
        let source_entry = self.entry(source).ok_or(crate::Error::NotFound)?;
        self.create(dest, &source_entry)?;

        if source_entry.entry_type == EntryType::File {
            self.repository.copy(
                &EntryKey(source.to_owned(), KeyType::Data),
                EntryKey(dest.to_owned(), KeyType::Data),
            )?;
        }

        Ok(())
    }

    /// Copy the tree of entries at `source` to `dest`.
    ///
    /// If `source` is a directory entry, this also copies its descendants.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    pub fn copy_tree(&mut self, source: &RelativePath, dest: &RelativePath) -> crate::Result<()> {
        self.copy(source, dest)?;

        match self.walk(source) {
            Ok(descendants) => {
                // We must convert to owned paths so we're not borrowing `self`.
                let mut owned_descendants = descendants
                    .into_iter()
                    .map(|path| path.to_relative_path_buf())
                    .collect::<Vec<_>>();

                // Sort paths in order by depth.
                owned_descendants.sort_by_key(|path| path.iter().count());

                for source_path in owned_descendants {
                    let relative_path = source_path.strip_prefix(source).unwrap();
                    let dest_path = dest.join(relative_path);
                    self.copy(&source_path, &dest_path)?;
                }

                Ok(())
            },
            Err(crate::Error::NotDirectory) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Return an unsorted list of paths which are children of `parent`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry at `parent`.
    /// - `Error::NotDirectory`: The entry at `parent` is not a directory.
    pub fn list(
        &mut self,
        parent: &RelativePath,
    ) -> crate::Result<Vec<&RelativePath>> {
        match self.entry(parent) {
            Some(entry) => if entry.entry_type != EntryType::Directory {
                return Err(crate::Error::NotDirectory);
            },
            None => return Err(crate::Error::NotFound),
        }

        Ok(
            self.repository
                .keys()
                .filter(|key| key.1 == KeyType::Metadata)
                .filter(move |key| key.0.parent() == Some(parent))
                .map(|key| key.0.as_ref())
                .collect()
        )
    }

    /// Return an unsorted list of paths which are descendants of `parent`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry at `parent`.
    /// - `Error::NotDirectory`: The entry at `parent` is not a directory.
    pub fn walk(
        &mut self,
        parent: &RelativePath,
    ) -> crate::Result<Vec<&RelativePath>> {
        match self.entry(parent) {
            Some(entry) => if entry.entry_type != EntryType::Directory {
                return Err(crate::Error::NotDirectory);
            },
            None => return Err(crate::Error::NotFound),
        }

        Ok(
            self.repository
                .keys()
                .filter(|key| key.1 == KeyType::Metadata)
                .filter(move |key| key.0.starts_with(parent))
                .map(|key| key.0.as_ref())
                .collect()
        )
    }

    /// Copy a file from the file system into the repository.
    ///
    /// This creates a repository entry at `dest` from the file at `source` and returns the entry.
    /// This does not remove the `source` file from the file system.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::Unsupported`: The file at `source` is not a regular file or directory.
    /// - `Error::Io`: An I/O error occurred.
    ///     - `ErrorKind::NotFound`: The `source` file does not exist.
    ///     - `ErrorKind::PermissionDenied`: The user lack permission to read the `source` file.
    pub fn archive(&mut self, source: &Path, dest: &RelativePath) -> crate::Result<Entry> {
        if self.exists(dest) {
            return Err(crate::Error::AlreadyExists);
        }

        let file_metadata = symlink_metadata(source)?;
        let file_type = file_metadata.file_type();

        let entry_type = if file_type.is_file() {
            EntryType::File
        } else if file_type.is_dir() {
            EntryType::Directory
        } else if file_type.is_symlink() {
            EntryType::Link { target: read_link(source)? }
        } else {
            return Err(crate::Error::Unsupported);
        };

        let entry = Entry {
            modified: file_metadata.modified()?,
            permissions: file_mode(&file_metadata),
            attributes: extended_attrs(&source)?,
            entry_type,
        };

        self.create(&dest, &entry)?;

        if entry.entry_type == EntryType::File {
            let mut object = self.open(dest)?;
            let mut file = File::open(source)?;
            copy(&mut file, &mut object)?;
            object.flush()?;
        }

        Ok(entry)
    }

    /// Copy a directory tree from the file system into the repository.
    ///
    /// This creates a tree of repository entries at `dest` from the directory tree at `source`.
    /// This does not remove the `source` directory or its descendants from the file system.
    ///
    /// If `source` is not a directory, this is the same as calling `archive`. If one of the files
    /// in the tree is not a regular file or directory, it is skipped.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::Io`: An I/O error occurred.
    ///     - `ErrorKind::NotFound`: The `source` file does not exist.
    ///     - `ErrorKind::PermissionDenied`: The user lacks permission to read the `source` file.
    pub fn archive_tree(&mut self, source: &Path, dest: &RelativePath) -> crate::Result<()> {
        // `WalkDir` includes `source` in the paths it iterates over.
        // It does not error if `source` is not a directory.
        for result in WalkDir::new(source) {
            let dir_entry = result.map_err(|error| io::Error::from(error))?;
            let relative_path = dir_entry.path().strip_prefix(source).unwrap();
            let entry_path = dest.join(RelativePath::from_path(relative_path).unwrap());
            match self.archive(dir_entry.path(), entry_path.as_relative_path()) {
                Ok(_) => continue,
                Err(crate::Error::Unsupported) => continue,
                Err(error) => return Err(error),
            }
        }

        Ok(())
    }

    /// Copy a file from the repository into the file system.
    ///
    /// This creates a file at `dest` from the archive entry at `source`. This does not remove the
    /// `source` entry from the repository.
    ///
    /// # Errors
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::Io`: An I/O error occurred.
    ///     - `ErrorKind::PermissionDenied`: The user lacks permission to create the `dest` file.
    ///     - `ErrorKind::AlreadyExists`: A file already exists at `dest`.
    pub fn extract(&mut self, source: &RelativePath, dest: &Path) -> crate::Result<()> {
        let entry = match self.entry(source) {
            Some(value) => value,
            None => return Err(crate::Error::NotFound),
        };

        // Create any necessary parent directories.
        if let Some(parent) = dest.parent() {
            create_dir_all(parent)?
        }

        // Create the file, directory, or symlink.
        match entry.entry_type {
            EntryType::File => {
                let mut object = self
                    .repository
                    .get(&EntryKey(source.to_owned(), KeyType::Data))
                    .expect("This entry has no data in the repository.");
                let mut file = OpenOptions::new().write(true).create_new(true).open(dest)?;
                copy(&mut object, &mut file)?;
            }
            EntryType::Directory => {
                create_dir(dest)?;
            },
            EntryType::Link { target } => {
                soft_link(dest, target.as_path())?;
            },
        }

        // Set the file metadata.
        set_file_mtime(dest, FileTime::from_system_time(entry.modified))?;
        if let Some(mode) = entry.permissions {
            set_file_mode(dest, mode)?;
        }
        set_extended_attrs(dest, entry.attributes)?;

        Ok(())
    }

    /// Copy a directory tree from the repository into the file system.
    ///
    /// This creates a directory tree at `dest` from the tree of repository entries at `source`.
    /// This does not remove the `source` entry or its descendants from the repository.
    ///
    /// If `source` is not a directory entry, this is the same as calling `extract`.
    ///
    /// # Errors
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::Io`: An I/O error occurred.
    ///     - `ErrorKind::PermissionDenied`: The user lacks permission to create the `dest` file.
    ///     - `ErrorKind::AlreadyExists`: A file already exists at `dest`.
    pub fn extract_tree(&mut self, source: &RelativePath, dest: &Path) -> crate::Result<()> {
        match self.walk(source) {
            Ok(descendants) => {
                // We must convert to owned paths so we're not borrowing `self`.
                let mut owned_descendants = descendants
                    .into_iter()
                    .map(|path| path.to_relative_path_buf())
                    .collect::<Vec<_>>();

                // Sort paths by depth.
                owned_descendants.sort_by_key(|path| path.iter().count());

                // Extract the root directory.
                self.extract(source, dest)?;

                // Extract the descendants.
                for entry_path in owned_descendants {
                    let file_path = entry_path.to_path(dest);
                    self.extract(entry_path.as_relative_path(), file_path.as_path())?;
                }

                Ok(())
            },
            Err(crate::Error::NotDirectory) => self.extract(source, dest),
            Err(error) => Err(error),
        }
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        self.repository.commit()
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// See `ObjectRepository::verify` for details.
    pub fn verify(&self) -> crate::Result<HashSet<&RelativePath>> {
        self.repository
            .verify()
            .map(|set| set.iter().map(|key| key.0.as_ref()).collect())
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepository::change_password` for details.
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repository.change_password(new_password);
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepositoryInfo {
        self.repository.info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// See `ObjectRepository::peek_info` for details.
    pub fn peek_info(store: S) -> crate::Result<RepositoryInfo> {
        ObjectRepository::<EntryKey, S>::peek_info(&store)
    }
}
