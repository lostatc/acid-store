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
use std::io::{self, copy, Read, Write};
use std::path::Path;

use filetime::{FileTime, set_file_mtime};
use lazy_static::lazy_static;
use relative_path::RelativePath;
use rmp_serde::{from_read, to_vec};
use uuid::Uuid;
use walkdir::WalkDir;

use crate::{DataStore, LockStrategy, Object, ObjectRepository, RepositoryConfig, RepositoryInfo};

use super::entry::{Entry, FileMetadata, FileType};
use super::platform::{extended_attrs, file_mode, set_extended_attrs, set_file_mode, soft_link};

lazy_static! {
    /// The current repository format version ID.
    static ref VERSION_ID: Uuid =
        Uuid::parse_str("307079ac-3563-11ea-bf84-309c230b49ee").unwrap();
}

/// A persistent file store.
///
/// This is a wrapper around `ObjectRepository` which allows it to function as a file archive like
/// ZIP or TAR.
///
/// This repository provides a high-level API for copying files between the file system and
/// repository through the methods `archive`, `archive_tree`, `extract`, and `extract_tree`. It is
/// also possible to manually add, remove, query, and modify files.
///
/// While files in the file system are identified by their `Path`, entries in the archive are
/// identified by a `RelativePath`. A `RelativePath` is a platform-independent path representation
/// that allows entries archived on one system to be extracted on another. Each `RelativePath` is
/// relative to the root of the archive.
///
/// `FileRepository` acts more like a file archive than a file system. A file's permissions
/// (`FileMetadata::permissions`) do not affect whether the file can be read from or written to
/// within the repository. A file's modification time (`FileMetadata::modified`) is not updated when
/// a file is modified in the repository. This metadata is only stored in the repository so that it
/// can be restored when the file is extracted to the file system.
///
/// Like `ObjectRepository`, changes made to the repository are not persisted to disk until `commit`
/// is called. For details about deduplication, compression, encryption, and locking, see
/// `ObjectRepository`.
pub struct FileRepository<S: DataStore> {
    repository: ObjectRepository<Entry, S>,
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
        let mut repository = ObjectRepository::create_repo(store, config, password)?;

        // Write the repository version.
        let mut object = repository.insert(Entry::Version);
        object.write_all(VERSION_ID.as_bytes())?;
        object.flush()?;
        drop(object);

        Ok(Self { repository })
    }

    /// Open the existing repository in the given data `store`.
    ///
    /// See `ObjectRepository::open_repo` for details.
    pub fn open_repo(
        store: S,
        password: Option<&[u8]>,
        strategy: LockStrategy,
    ) -> crate::Result<Self> {
        let mut repository = ObjectRepository::open_repo(store, password, strategy)?;

        // Read the repository version to see if this is a compatible repository.
        let mut object = repository
            .get(&Entry::Version)
            .ok_or(crate::Error::NotFound)?;
        let mut version_buffer = Vec::new();
        object.read_to_end(&mut version_buffer)?;
        drop(object);

        let version =
            Uuid::from_slice(version_buffer.as_slice()).map_err(|_| crate::Error::Corrupt)?;
        if version != *VERSION_ID {
            return Err(crate::Error::UnsupportedVersion);
        }

        Ok(Self { repository })
    }

    /// Return whether there is a file at `path`.
    pub fn exists(&self, path: &RelativePath) -> bool {
        self.repository.contains(&Entry::Metadata(path.to_owned()))
    }

    /// Add a new empty file, directory, or symlink to the repository at the given `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `path` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already a file at `path`.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create(&mut self, path: &RelativePath, metadata: &FileMetadata) -> crate::Result<()> {
        if self.exists(path) {
            return Err(crate::Error::AlreadyExists);
        }

        let data_key = Entry::Data(path.to_owned());
        let metadata_key = Entry::Metadata(path.to_owned());

        // Check if the parent directory exists.
        if let Some(parent) = path.parent() {
            match self.metadata(parent) {
                Some(metadata) => {
                    if !metadata.is_directory() {
                        return Err(crate::Error::InvalidPath);
                    }
                }
                None => return Err(crate::Error::InvalidPath),
            }
        }

        // Remove any existing data object and add a new one if this is a file.
        if metadata.is_file() {
            self.repository.insert(data_key);
        } else {
            self.repository.remove(&data_key);
        }

        // Write the metadata for the file.
        let mut object = self.repository.insert(metadata_key);
        let serialized_metadata = to_vec(&metadata).expect("Could not serialize file metadata.");
        object.write_all(serialized_metadata.as_slice())?;
        object.flush()?;

        Ok(())
    }

    /// Add a new empty file, directory, or symlink to the repository at the given `path`.
    ///
    /// This also creates any missing parent directories.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: There is already a file at `path`.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create_parents(
        &mut self,
        path: &RelativePath,
        metadata: &FileMetadata,
    ) -> crate::Result<()> {
        let mut ancestor = path.parent();
        while let Some(directory) = ancestor {
            if self.exists(directory) {
                break;
            }
            self.create(directory, &FileMetadata::directory())?;
            ancestor = directory.parent();
        }

        self.create(path, metadata)
    }

    /// Remove the file with the given `path` from the repository.
    ///
    /// The space used by the given file isn't freed and made available for new files until `commit`
    /// is called.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no file with the given `path`.
    /// - `Error::NotEmpty`: The file is a directory which is not empty.
    pub fn remove(&mut self, path: &RelativePath) -> crate::Result<()> {
        if !self.exists(path) {
            return Err(crate::Error::NotFound);
        }

        match self.list(path) {
            Ok(mut children) => {
                if children.next().is_some() {
                    return Err(crate::Error::NotEmpty);
                }
            }
            Err(crate::Error::NotDirectory) => (),
            Err(error) => return Err(error),
        }

        self.repository.remove(&Entry::Data(path.to_owned()));
        self.repository.remove(&Entry::Metadata(path.to_owned()));

        Ok(())
    }

    /// Remove the file with the given `path` and its descendants from the repository.
    ///
    /// The space used by the given file isn't freed and made available for new files until `commit`
    /// is called.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no file with the given `path`.
    pub fn remove_tree(&mut self, path: &RelativePath) -> crate::Result<()> {
        let descendants = match self.walk(path) {
            Ok(descendants) => {
                // We must convert to owned paths so we're not borrowing `self`.
                let mut owned_descendants =
                    descendants.map(|path| path.to_owned()).collect::<Vec<_>>();

                // Sort paths in reverse order by depth.
                owned_descendants.sort_by_key(|path| Reverse(path.iter().count()));

                owned_descendants
            }
            Err(crate::Error::NotDirectory) => Vec::new(),
            Err(error) => return Err(error),
        };

        // Extract the descendants.
        for descendant in descendants {
            self.remove(descendant.as_ref())?;
        }

        // Extract the root directory.
        self.remove(path)
    }

    /// Return the metadata for the file at `path` or `None` if there is none.
    pub fn metadata(&mut self, path: &RelativePath) -> Option<FileMetadata> {
        let object = self.repository.get(&Entry::Metadata(path.to_owned()))?;

        Some(from_read(object).expect("Could not deserialize file metadata."))
    }

    /// Set the `metadata` for the file at `path`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no file at `path`.
    pub fn set_metadata(
        &mut self,
        path: &RelativePath,
        metadata: &FileMetadata,
    ) -> crate::Result<()> {
        let mut object = self
            .repository
            .get(&Entry::Metadata(path.to_owned()))
            .ok_or(crate::Error::NotFound)?;

        let serialized_metadata = to_vec(metadata).expect("Could not serialize file metadata.");
        object.write_all(serialized_metadata.as_slice())?;
        object.flush()?;

        Ok(())
    }

    /// Return an `Object` for reading and writing the contents of the regular file at `path`.
    ///
    /// Writing to the returned object does not update the `FileMetadata::modified` value for the
    /// file.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no file with the given `path`.
    /// - `Error::NotFile`: The file is not a regular file.
    pub fn open(&mut self, path: &RelativePath) -> crate::Result<Object<Entry, S>> {
        let metadata = self.metadata(path).ok_or(crate::Error::NotFound)?;
        if !metadata.is_file() {
            return Err(crate::Error::NotFile);
        }
        let object = self
            .repository
            .get(&Entry::Data(path.to_owned()))
            .expect("There is no object associated with this file.");
        Ok(object)
    }

    /// Copy the file at `source` to `dest`.
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// This copies the file from one location in the repository to another. To copy files from the
    /// file system to the repository, see `archive`. To copy files from the repository to the file
    /// system, see `extract`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the files.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no file at `source`.
    /// - `Error::AlreadyExists`: There is already a file at `dest`.
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    pub fn copy(&mut self, source: &RelativePath, dest: &RelativePath) -> crate::Result<()> {
        let source_metadata = self.metadata(source).ok_or(crate::Error::NotFound)?;
        self.create(dest, &source_metadata)?;

        if source_metadata.is_file() {
            self.repository.copy(
                &Entry::Data(source.to_owned()),
                Entry::Data(dest.to_owned()),
            )?;
        }

        Ok(())
    }

    /// Copy the tree of files at `source` to `dest`.
    ///
    /// If `source` is a directory, this also copies its descendants.
    ///
    /// This copies the file from one location in the repository to another. To copy files from the
    /// file system to the repository, see `archive`. To copy files from the repository to the file
    /// system, see `extract`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the files.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no file at `source`.
    /// - `Error::AlreadyExists`: There is already a file at `dest`.
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    pub fn copy_tree(&mut self, source: &RelativePath, dest: &RelativePath) -> crate::Result<()> {
        // Copy the root directory.
        self.copy(source, dest)?;

        let descendants = match self.walk(source) {
            Ok(descendants) => {
                // We must convert to owned paths so we're not borrowing `self`.
                let mut owned_descendants =
                    descendants.map(|path| path.to_owned()).collect::<Vec<_>>();

                // Sort paths in order by depth.
                owned_descendants.sort_by_key(|path| path.iter().count());

                owned_descendants
            }
            Err(crate::Error::NotDirectory) => return Ok(()),
            Err(error) => return Err(error),
        };

        // Copy the descendants.
        for source_path in descendants {
            let relative_path = source_path.strip_prefix(source).unwrap();
            let dest_path = dest.join(relative_path);
            self.copy(&source_path, &dest_path)?;
        }

        Ok(())
    }

    /// Return an unsorted iterator of paths which are children of `parent`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no file at `parent`.
    /// - `Error::NotDirectory`: The file at `parent` is not a directory.
    pub fn list<'a>(
        &'a mut self,
        parent: &'a RelativePath,
    ) -> crate::Result<impl Iterator<Item=&'a RelativePath> + 'a> {
        match self.metadata(parent) {
            Some(metadata) => {
                if !metadata.is_directory() {
                    return Err(crate::Error::NotDirectory);
                }
            }
            None => return Err(crate::Error::NotFound),
        }

        let children = self.repository.keys().filter_map(move |entry| match entry {
            Entry::Metadata(path) => {
                if path.parent() == Some(parent) {
                    Some(path.as_ref())
                } else {
                    None
                }
            }
            _ => None,
        });

        Ok(children)
    }

    /// Return an unsorted iterator of paths which are descendants of `parent`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no file at `parent`.
    /// - `Error::NotDirectory`: The file at `parent` is not a directory.
    pub fn walk<'a>(
        &'a mut self,
        parent: &'a RelativePath,
    ) -> crate::Result<impl Iterator<Item=&'a RelativePath> + 'a> {
        match self.metadata(parent) {
            Some(metadata) => {
                if !metadata.is_directory() {
                    return Err(crate::Error::NotDirectory);
                }
            }
            None => return Err(crate::Error::NotFound),
        }

        let descendants = self.repository.keys().filter_map(move |entry| match entry {
            Entry::Metadata(path) => {
                if path.starts_with(parent) {
                    Some(path.as_relative_path())
                } else {
                    None
                }
            }
            _ => None,
        });

        Ok(descendants)
    }

    /// Copy a file from the file system into the repository.
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already a file at `dest`.
    /// - `Error::FileType`: The file at `source` is not a regular file or directory.
    /// - `Error::Io`: An I/O error occurred.
    ///     - `ErrorKind::NotFound`: The `source` file does not exist.
    ///     - `ErrorKind::PermissionDenied`: The user lack permission to read the `source` file.
    pub fn archive(&mut self, source: &Path, dest: &RelativePath) -> crate::Result<()> {
        if self.exists(dest) {
            return Err(crate::Error::AlreadyExists);
        }

        let file_metadata = symlink_metadata(source)?;
        let file_type = file_metadata.file_type();

        let file_type = if file_type.is_file() {
            FileType::File
        } else if file_type.is_dir() {
            FileType::Directory
        } else if file_type.is_symlink() {
            FileType::Link {
                target: read_link(source)?,
            }
        } else {
            return Err(crate::Error::FileType);
        };

        let metadata = FileMetadata {
            modified: file_metadata.modified()?,
            permissions: file_mode(&file_metadata),
            attributes: extended_attrs(&source)?,
            file_type,
        };

        self.create(&dest, &metadata)?;

        if metadata.is_file() {
            let mut object = self.open(dest)?;
            let mut file = File::open(source)?;
            copy(&mut file, &mut object)?;
            object.flush()?;
        }

        Ok(())
    }

    /// Copy a directory tree from the file system into the repository.
    ///
    /// If `source` is a directory, this also copies its descendants. If `source` is not a
    /// directory, this is the same as calling `archive`. If one of the files in the tree is not a
    /// regular file, directory, or symbolic link, it is skipped.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already a file at `dest`.
    /// - `Error::Io`: An I/O error occurred.
    ///     - `ErrorKind::NotFound`: The `source` file does not exist.
    ///     - `ErrorKind::PermissionDenied`: The user lacks permission to read the `source` file.
    pub fn archive_tree(&mut self, source: &Path, dest: &RelativePath) -> crate::Result<()> {
        // `WalkDir` includes `source` in the paths it iterates over.
        // It does not error if `source` is not a directory.
        for result in WalkDir::new(source) {
            let dir_entry = result.map_err(|error| io::Error::from(error))?;
            let relative_path = dir_entry.path().strip_prefix(source).unwrap();
            let file_path = dest.join(RelativePath::from_path(relative_path).unwrap());
            match self.archive(dir_entry.path(), file_path.as_relative_path()) {
                Ok(_) => continue,
                Err(crate::Error::FileType) => continue,
                Err(error) => return Err(error),
            }
        }

        Ok(())
    }

    /// Copy a file from the repository into the file system.
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// # Errors
    /// - `Error::NotFound`: The `source` file does not exist.
    /// - `Error::Io`: An I/O error occurred.
    ///     - `ErrorKind::PermissionDenied`: The user lacks permission to create the `dest` file.
    ///     - `ErrorKind::AlreadyExists`: A file already exists at `dest`.
    pub fn extract(&mut self, source: &RelativePath, dest: &Path) -> crate::Result<()> {
        let metadata = match self.metadata(source) {
            Some(value) => value,
            None => return Err(crate::Error::NotFound),
        };

        // Create any necessary parent directories.
        if let Some(parent) = dest.parent() {
            create_dir_all(parent)?
        }

        // Create the file, directory, or symlink.
        match metadata.file_type {
            FileType::File => {
                let mut object = self
                    .repository
                    .get(&Entry::Data(source.to_owned()))
                    .expect("This file has no data in the repository.");
                let mut file = OpenOptions::new().write(true).create_new(true).open(dest)?;
                copy(&mut object, &mut file)?;
            }
            FileType::Directory => {
                create_dir(dest)?;
            }
            FileType::Link { target } => {
                soft_link(dest, target.as_path())?;
            }
        }

        // Set the file metadata.
        set_file_mtime(dest, FileTime::from_system_time(metadata.modified))?;
        if let Some(mode) = metadata.permissions {
            set_file_mode(dest, mode)?;
        }
        set_extended_attrs(dest, metadata.attributes)?;

        Ok(())
    }

    /// Copy a directory tree from the repository into the file system.
    ///
    /// If `source` is a directory, this also copies its descendants. If `source` is not a
    /// directory, this is the same as calling `extract`.
    ///
    /// # Errors
    /// - `Error::NotFound`: The `source` file does not exist.
    /// - `Error::Io`: An I/O error occurred.
    ///     - `ErrorKind::PermissionDenied`: The user lacks permission to create the `dest` file.
    ///     - `ErrorKind::AlreadyExists`: A file already exists at `dest`.
    pub fn extract_tree(&mut self, source: &RelativePath, dest: &Path) -> crate::Result<()> {
        let descendants = match self.walk(source) {
            Ok(descendants) => {
                // We must convert to owned paths so we're not borrowing `self`.
                let mut owned_descendants = descendants
                    .into_iter()
                    .map(|path| path.to_owned())
                    .collect::<Vec<_>>();

                // Sort paths by depth.
                owned_descendants.sort_by_key(|path| path.iter().count());

                owned_descendants
            }
            Err(crate::Error::NotDirectory) => Vec::new(),
            Err(error) => return Err(error),
        };

        // Extract the root directory.
        self.extract(source, dest)?;

        // Extract the descendants.
        for relative_path in descendants {
            let file_path = relative_path.to_path(dest);
            self.extract(relative_path.as_ref(), file_path.as_path())?;
        }

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        self.repository.commit()
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of paths of files with corrupt data or metadata.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&self) -> crate::Result<HashSet<&RelativePath>> {
        let paths = self
            .repository
            .verify()?
            .iter()
            .filter_map(|entry| match entry {
                Entry::Data(path) => Some(path.as_ref()),
                Entry::Metadata(path) => Some(path.as_ref()),
                _ => None,
            })
            .collect();

        Ok(paths)
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
        ObjectRepository::<Entry, S>::peek_info(&store)
    }
}
