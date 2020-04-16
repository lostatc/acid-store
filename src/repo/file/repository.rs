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

use std::cmp::Reverse;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::{create_dir, create_dir_all, File, metadata, OpenOptions};
use std::io::{self, copy, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use path_slash::PathExt;
use rmp_serde::{from_read, to_vec};
use uuid::Uuid;
use walkdir::WalkDir;

use lazy_static::lazy_static;

use crate::repo::{
    LockStrategy, Object, ObjectRepository, ReadOnlyObject, RepositoryConfig, RepositoryInfo,
    RepositoryStats,
};
use crate::store::DataStore;

use super::entry::{Entry, EntryKey, FileType};
use super::metadata::FileMetadata;

lazy_static! {
    /// The current repository format version ID.
    static ref VERSION_ID: Uuid =
        Uuid::parse_str("307079ac-3563-11ea-bf84-309c230b49ee").unwrap();

    /// The parent of a relative path with no parent.
    static ref EMPTY_PARENT: &'static Path = &Path::new("");
}

/// A path to a file in a `FileRepository`.
pub type EntryPath = Path;

/// A virtual file system.
///
/// This is a wrapper around `ObjectRepository` which allows it to function as a virtual file
/// system.
///
/// A `FileRepository` is composed of `Entry` values which represent either a regular file or a
/// directory. Files in the file system can be copied into the repository using `archive` and
/// `archive_tree`, and entries in the repository can be copied to the file system using `extract`
/// and `extract_tree`. It is also possible to manually add, remove, query, and modify entries.
///
/// This repository is designed so that files archived on one platform can be extracted on another
/// platform. Because file metadata is very platform-dependent, you can choose what metadata you
/// want to store by implementing the `FileMetadata` trait. If you don't need to store file
/// metadata, you can use the `NoMetadata` implementation. If you attempt to read an entry using a
/// different `FileMetadata` implementation than it was stored with, it will fail to deserialize and
/// return an error.
///
/// Entries in the repository are located using a `Path`, just like files in the file system. To
/// make it clear whether a function is expecting an entry path or a file path, the `EntryPath`
/// alias is used to refer to a path to an entry in the repository. The repository accepts both '\\'
/// and '/' as path separators in an `EntryPath`, and will automatically convert between them.
/// `EntryPath` values must always be relative paths, relative to the root of the repository.
///
/// Like `ObjectRepository`, changes made to the repository are not persisted to the data store
/// until `commit` is called. For details about deduplication, compression, encryption, and locking,
/// see `ObjectRepository`.
#[derive(Debug)]
pub struct FileRepository<S: DataStore, M: FileMetadata> {
    repository: ObjectRepository<EntryKey, S>,
    marker: PhantomData<M>,
}

impl<S: DataStore, M: FileMetadata> FileRepository<S, M> {
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
        let mut object = repository.insert(EntryKey::Version);
        object.write_all(VERSION_ID.as_bytes())?;
        object.flush()?;
        drop(object);

        Ok(Self {
            repository,
            marker: PhantomData,
        })
    }

    /// Open the existing repository in the given data `store`.
    ///
    /// See `ObjectRepository::open_repo` for details.
    pub fn open_repo(
        store: S,
        strategy: LockStrategy,
        password: Option<&[u8]>,
    ) -> crate::Result<Self> {
        let repository = ObjectRepository::open_repo(store, strategy, password)?;

        // Read the repository version to see if this is a compatible repository.
        let mut object = repository
            .get(&EntryKey::Version)
            .ok_or(crate::Error::NotFound)?;
        let mut version_buffer = Vec::new();
        object.read_to_end(&mut version_buffer)?;
        drop(object);

        let version =
            Uuid::from_slice(version_buffer.as_slice()).map_err(|_| crate::Error::Corrupt)?;
        if version != *VERSION_ID {
            return Err(crate::Error::UnsupportedFormat);
        }

        Ok(Self {
            repository,
            marker: PhantomData,
        })
    }

    /// Convert the given `path` to a platform-agnostic path and check if it is relative.
    fn convert_path(path: impl AsRef<EntryPath>) -> crate::Result<PathBuf> {
        if path.as_ref().has_root() {
            Err(crate::Error::InvalidPath)
        } else {
            Ok(path.as_ref().to_slash_lossy().into())
        }
    }

    /// Return whether there is an entry at `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    pub fn exists(&self, path: impl AsRef<EntryPath>) -> crate::Result<bool> {
        let path = Self::convert_path(path)?;
        Ok(self.repository.contains(&EntryKey::Entry(path)))
    }

    /// Add a new empty file or directory entry to the repository at the given `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    /// - `Error::InvalidPath`: The parent of `path` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `path`.
    /// - `Error::Serialize`: The new file metadata could not be serialized.
    /// - `Error::Deserialize`: The old file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create(&mut self, path: impl AsRef<EntryPath>, entry: &Entry<M>) -> crate::Result<()> {
        let path = Self::convert_path(path)?;

        if self.exists(&path)? {
            return Err(crate::Error::AlreadyExists);
        }

        let data_key = EntryKey::Data(path.to_owned());
        let entry_key = EntryKey::Entry(path.to_owned());

        // Check if the parent directory exists.
        match path.parent() {
            Some(parent) if parent != *EMPTY_PARENT => match self.entry(parent) {
                Ok(parent_entry) if !parent_entry.is_directory() => {
                    return Err(crate::Error::InvalidPath)
                }
                Err(crate::Error::NotFound) => return Err(crate::Error::InvalidPath),
                Err(error) => return Err(error),
                _ => (),
            },
            _ => (),
        }

        // Remove any existing data object and add a new one if this is a file.
        if entry.is_file() {
            self.repository.insert(data_key);
        } else {
            self.repository.remove(&data_key);
        }

        // Write the metadata for the file.
        let mut object = self.repository.insert(entry_key);
        let serialized_entry = to_vec(entry).map_err(|_| crate::Error::Serialize)?;
        object.write_all(serialized_entry.as_slice())?;
        object.flush()?;

        Ok(())
    }

    /// Add a new empty file or directory entry to the repository at the given `path`.
    ///
    /// This also creates any missing parent directories.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    /// - `Error::AlreadyExists`: There is already an entry at `path`.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Serialize`: The new file metadata could not be serialized.
    /// - `Error::Deserialize`: The old file metadata could not be deserialized.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create_parents(
        &mut self,
        path: impl AsRef<EntryPath>,
        entry: &Entry<M>,
    ) -> crate::Result<()> {
        let path = Self::convert_path(path)?;

        let parent = match path.parent() {
            Some(parent) if parent != *EMPTY_PARENT => parent,
            _ => return self.create(path, entry),
        };

        let mut ancestor = PathBuf::new();
        for component in parent.iter() {
            ancestor.push(component);
            match self.create(&ancestor, &Entry::directory()) {
                Err(crate::Error::AlreadyExists) => (),
                Err(error) => return Err(error),
                _ => (),
            }
        }

        self.create(path, entry)
    }

    /// Remove the entry with the given `path` from the repository.
    ///
    /// The space used by the given entry isn't freed and made available for new entries until
    /// `commit` is called.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotEmpty`: The entry is a directory which is not empty.
    pub fn remove(&mut self, path: impl AsRef<EntryPath>) -> crate::Result<()> {
        let path = Self::convert_path(path)?;

        if !self.exists(&path)? {
            return Err(crate::Error::NotFound);
        }

        match self.list(&path) {
            Ok(mut children) => {
                if children.next().is_some() {
                    return Err(crate::Error::NotEmpty);
                }
            }
            Err(crate::Error::NotDirectory) => (),
            Err(error) => return Err(error),
        }

        self.repository.remove(&EntryKey::Data(path.to_owned()));
        self.repository.remove(&EntryKey::Entry(path));

        Ok(())
    }

    /// Remove the entry with the given `path` and its descendants from the repository.
    ///
    /// The space used by the given entry isn't freed and made available for new entries until
    /// `commit` is called.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    /// - `Error::NotFound`: There is no entry with the given `path`.
    pub fn remove_tree(&mut self, path: impl AsRef<EntryPath>) -> crate::Result<()> {
        let path = Self::convert_path(path)?;

        let mut descendants = match self.walk(&path) {
            Ok(descendants) => descendants.map(|path| path.to_owned()).collect::<Vec<_>>(),
            Err(crate::Error::NotDirectory) => Vec::new(),
            Err(error) => return Err(error),
        };

        // Sort paths in reverse order by depth.
        descendants.sort_by_key(|path| Reverse(path.iter().count()));

        // Extract the descendants.
        for descendant in descendants {
            self.remove(descendant)?;
        }

        // Extract the root directory.
        self.remove(path)
    }

    /// Return the entry at `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    /// - `Error::NotFound`: There is no entry at `path`.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn entry(&self, path: impl AsRef<EntryPath>) -> crate::Result<Entry<M>> {
        let path = Self::convert_path(path)?;

        let mut object = self
            .repository
            .get(&EntryKey::Entry(path))
            .ok_or(crate::Error::NotFound)?;

        // Catch any errors before passing to `from_read`.
        let mut serialized_entry = Vec::with_capacity(object.size() as usize);
        object.read_to_end(&mut serialized_entry)?;

        Ok(from_read(serialized_entry.as_slice()).map_err(|_| crate::Error::Deserialize)?)
    }

    /// Set the file `metadata` for the entry at `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    /// - `Error::NotFound`: There is no entry at `path`.
    /// - `Error::Serialize`: The new file metadata could not be serialized.
    /// - `Error::Deserialize`: The old file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn set_metadata(&mut self, path: impl AsRef<EntryPath>, metadata: M) -> crate::Result<()> {
        let path = Self::convert_path(path)?;

        let mut entry = self.entry(&path)?;
        entry.metadata = metadata;

        let mut object = self
            .repository
            .get_mut(&EntryKey::Entry(path))
            .ok_or(crate::Error::NotFound)?;

        let serialized_entry = to_vec(&entry).map_err(|_| crate::Error::Serialize)?;
        object.write_all(serialized_entry.as_slice())?;
        object.flush()?;

        Ok(())
    }

    /// Return an `Object` for reading the contents of the file entry at `path`.
    ///
    /// The returned object provides read-only access to the file. To get read-write access, use
    /// `open_mut`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotFile`: The entry does not represent a regular file.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn open(&self, path: impl AsRef<EntryPath>) -> crate::Result<ReadOnlyObject<EntryKey, S>> {
        let path = Self::convert_path(path)?;

        let entry = self.entry(&path)?;
        if !entry.is_file() {
            return Err(crate::Error::NotFile);
        }

        let object = self
            .repository
            .get(&EntryKey::Data(path))
            .expect("There is no object associated with this file.");

        Ok(object)
    }

    /// Return an `Object` for reading and writing the contents of the file entry at `path`.
    ///
    /// The returned object provides read-write access to the file. To get read-only access, use
    /// `open`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is absolute.
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotFile`: The entry does not represent a regular file.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn open_mut(&mut self, path: impl AsRef<EntryPath>) -> crate::Result<Object<EntryKey, S>> {
        let path = Self::convert_path(path)?;

        let entry = self.entry(&path)?;
        if !entry.is_file() {
            return Err(crate::Error::NotFile);
        }

        let object = self
            .repository
            .get_mut(&EntryKey::Data(path))
            .expect("There is no object associated with this file.");

        Ok(object)
    }

    /// Copy the entry at `source` to `dest`.
    ///
    /// If `source` is a directory entry, its descendants are not copied.
    ///
    /// This copies the entry from one location in the repository to another. To copy files from the
    /// file system to the repository, see `archive`. To copy files from the repository to the file
    /// system, see `extract`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the files.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `source` or `dest` is absolute.
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn copy(
        &mut self,
        source: impl AsRef<EntryPath>,
        dest: impl AsRef<EntryPath>,
    ) -> crate::Result<()> {
        let source = Self::convert_path(source)?;
        let dest = Self::convert_path(dest)?;

        let source_entry = self.entry(&source)?;
        self.create(&dest, &source_entry)?;

        if source_entry.is_file() {
            let data_key = EntryKey::Data(dest);
            self.repository.remove(&data_key);
            self.repository.copy(&EntryKey::Data(source), data_key)?;
        }

        Ok(())
    }

    /// Copy the tree of entries at `source` to `dest`.
    ///
    /// If `source` is a directory entry, this also copies its descendants.
    ///
    /// This copies entries from one location in the repository to another. To copy files from the
    /// file system to the repository, see `archive`. To copy files from the repository to the file
    /// system, see `extract`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the files.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `source` or `dest` is absolute.
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn copy_tree(
        &mut self,
        source: impl AsRef<EntryPath>,
        dest: impl AsRef<EntryPath>,
    ) -> crate::Result<()> {
        let source = Self::convert_path(source)?;
        let dest = Self::convert_path(dest)?;

        // Copy the root directory.
        self.copy(&source, &dest)?;

        let mut descendants = match self.walk(&source) {
            Ok(descendants) => descendants.map(|path| path.to_owned()).collect::<Vec<_>>(),
            Err(crate::Error::NotDirectory) => return Ok(()),
            Err(error) => return Err(error),
        };

        // Sort paths in order by depth.
        descendants.sort_by_key(|path| path.iter().count());

        // Copy the descendants.
        for source_path in descendants {
            let relative_path = source_path.strip_prefix(&source).unwrap();
            let dest_path = dest.join(relative_path);
            self.copy(&source_path, &dest_path)?;
        }

        Ok(())
    }

    /// Return an unsorted iterator of paths which are children of `parent`.
    ///
    /// The returned paths do not include `parent`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `parent` is absolute.
    /// - `Error::NotFound`: There is no entry at `parent`.
    /// - `Error::NotDirectory`: The entry at `parent` is not a directory.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn list<'a>(
        &'a self,
        parent: impl AsRef<EntryPath>,
    ) -> crate::Result<impl Iterator<Item = &'a EntryPath> + 'a> {
        let parent = Self::convert_path(parent)?;

        if !self.entry(&parent)?.is_directory() {
            return Err(crate::Error::NotDirectory);
        }

        let children = self.repository.keys().filter_map(move |entry| match entry {
            EntryKey::Entry(path) if path.parent() == Some(&parent) => Some(path.as_path()),
            _ => None,
        });

        Ok(children)
    }

    /// Return an unsorted iterator of paths which are descendants of `parent`.
    ///
    /// The returned paths do not include `parent`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `parent` is absolute.
    /// - `Error::NotFound`: There is no entry at `parent`.
    /// - `Error::NotDirectory`: The entry at `parent` is not a directory.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn walk<'a>(
        &'a self,
        parent: impl AsRef<EntryPath>,
    ) -> crate::Result<impl Iterator<Item = &'a EntryPath> + 'a> {
        let parent = Self::convert_path(parent)?;

        if !self.entry(&parent)?.is_directory() {
            return Err(crate::Error::NotDirectory);
        }

        let descendants = self.repository.keys().filter_map(move |entry| match entry {
            EntryKey::Entry(path) if path.starts_with(&parent) && path != &parent => {
                Some(path.as_path())
            }
            _ => None,
        });

        Ok(descendants)
    }

    /// Copy a file from the file system into the repository.
    ///
    /// The `source` file's metadata will be applied to the `dest` entry according to the selected
    /// `FileMetadata` implementation.
    ///
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `dest` is absolute.
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::FileType`: The file at `source` is not a regular file or directory.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn archive(
        &mut self,
        source: impl AsRef<Path>,
        dest: impl AsRef<EntryPath>,
    ) -> crate::Result<()> {
        let dest = Self::convert_path(dest)?;

        if self.exists(&dest)? {
            return Err(crate::Error::AlreadyExists);
        }

        let file_metadata = metadata(&source)?;

        let file_type = if file_metadata.is_file() {
            FileType::File
        } else if file_metadata.is_dir() {
            FileType::Directory
        } else {
            return Err(crate::Error::FileType);
        };

        let entry = Entry {
            file_type,
            metadata: M::read_metadata(source.as_ref())?,
        };

        self.create(&dest, &entry)?;

        if entry.is_file() {
            let mut object = self.open_mut(&dest)?;
            let mut file = File::open(&source)?;
            copy(&mut file, &mut object)?;
            object.flush()?;
        }

        Ok(())
    }

    /// Copy a directory tree from the file system into the repository.
    ///
    /// The `source` file's metadata will be applied to the `dest` entry according to the selected
    /// `FileMetadata` implementation.
    ///
    /// If `source` is a directory, this also copies its descendants. If `source` is not a
    /// directory, this is the same as calling `archive`. If one of the files in the tree is not a
    /// regular file or directory, it is skipped.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `dest` is absolute.
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn archive_tree(
        &mut self,
        source: impl AsRef<Path>,
        dest: impl AsRef<EntryPath>,
    ) -> crate::Result<()> {
        let dest = Self::convert_path(dest)?;

        // `WalkDir` includes `source` in the paths it iterates over.
        // It does not error if `source` is not a directory.
        for result in WalkDir::new(&source) {
            let dir_entry = result.map_err(io::Error::from)?;
            let relative_path = dir_entry.path().strip_prefix(&source).unwrap();
            match self.archive(dir_entry.path(), dest.join(relative_path)) {
                Ok(_) => continue,
                Err(crate::Error::FileType) => continue,
                Err(error) => return Err(error),
            }
        }

        Ok(())
    }

    /// Copy an entry from the repository into the file system.
    ///
    /// The `source` entry's metadata will be applied to the `dest` file according to the selected
    /// `FileMetadata` implementation.
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `source` is absolute.
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn extract(
        &mut self,
        source: impl AsRef<EntryPath>,
        dest: impl AsRef<Path>,
    ) -> crate::Result<()> {
        let source = Self::convert_path(source)?;

        if dest.as_ref().exists() {
            return Err(crate::Error::AlreadyExists);
        }

        let entry = self.entry(&source)?;

        // Create any necessary parent directories.
        if let Some(parent) = dest.as_ref().parent() {
            create_dir_all(parent)?
        }

        // Create the file or directory.
        match entry.file_type {
            FileType::File => {
                let mut object = self.open(&source)?;
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&dest)?;
                copy(&mut object, &mut file)?;
            }
            FileType::Directory => {
                create_dir(&dest)?;
            }
        }

        // Set the file metadata.
        entry.metadata.write_metadata(dest.as_ref())?;

        Ok(())
    }

    /// Copy a tree of entries from the repository into the file system.
    ///
    /// The `source` entry's metadata will be applied to the `dest` file according to the selected
    /// `FileMetadata` implementation.
    ///
    /// If `source` is a directory, this also copies its descendants. If `source` is not a
    /// directory, this is the same as calling `extract`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `source` is absolute.
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn extract_tree(
        &mut self,
        source: impl AsRef<EntryPath>,
        dest: impl AsRef<Path>,
    ) -> crate::Result<()> {
        let source = Self::convert_path(source)?;

        let relative_descendants = match self.walk(&source) {
            Ok(descendants) => {
                let mut relative_descendants = descendants
                    .map(|path| path.strip_prefix(&source).unwrap().to_owned())
                    .collect::<Vec<_>>();

                // Sort paths by depth.
                relative_descendants.sort_by_key(|path| path.iter().count());
                relative_descendants
            }
            Err(crate::Error::NotDirectory) => Vec::new(),
            Err(error) => return Err(error),
        };

        // Extract the root directory.
        self.extract(&source, &dest)?;

        // Extract the descendants.
        for descendant in relative_descendants {
            self.extract(source.join(&descendant), dest.as_ref().join(&descendant))?;
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
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&self) -> crate::Result<HashSet<&EntryPath>> {
        let paths = self
            .repository
            .verify()?
            .iter()
            .filter_map(|entry| match entry {
                EntryKey::Data(path) => Some(path.as_path()),
                EntryKey::Entry(path) => Some(path.as_path()),
                _ => None,
            })
            .collect();

        Ok(paths)
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepository::change_password` for details.
    #[cfg(feature = "encryption")]
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
    pub fn peek_info(store: &mut S) -> crate::Result<RepositoryInfo> {
        ObjectRepository::<EntryKey, S>::peek_info(store)
    }

    /// Calculate statistics about the repository.
    pub fn stats(&self) -> RepositoryStats {
        self.repository.stats()
    }

    /// Consume this repository and return the wrapped `DataStore`.
    pub fn into_store(self) -> S {
        self.repository.into_store()
    }
}
