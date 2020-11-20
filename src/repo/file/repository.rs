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

use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::{create_dir, create_dir_all, metadata, File, OpenOptions};
use std::io::{self, copy, Write};
use std::marker::PhantomData;
use std::path::Path;

use hex_literal::hex;
use lazy_static::lazy_static;
use relative_path::{RelativePath, RelativePathBuf};
use uuid::Uuid;
use walkdir::WalkDir;

use crate::repo::common::check_version;
use crate::repo::object::ObjectRepo;
use crate::repo::{ConvertRepo, Object, ReadOnlyObject, RepoInfo};
use crate::store::DataStore;

use super::entry::{Entry, EntryHandle, EntryType, FileType};
use super::metadata::{FileMetadata, NoMetadata};
use super::path_tree::PathTree;
use super::special::{NoSpecialType, SpecialType};

lazy_static! {
    /// The parent of a relative path with no parent.
    static ref EMPTY_PARENT: &'static RelativePath = &RelativePath::new("");
}

/// The ID of the managed object which stores the table of keys for the repository.
const TABLE_OBJECT_ID: Uuid = Uuid::from_bytes(hex!("9c114e82 bd64 11ea 9872 ab55cbe7bb41"));

/// The current repository format version ID.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("36f6c626 d029 11ea 91e5 4f0aba7bed31"));

/// A virtual file system.
#[derive(Debug)]
pub struct FileRepo<S, T = NoSpecialType, M = NoMetadata>
where
    S: DataStore,
    T: SpecialType,
    M: FileMetadata,
{
    /// The backing repository.
    repository: ObjectRepo<S>,

    /// A map of relative file paths to the handles of the objects containing their entries.
    path_table: PathTree<EntryHandle>,

    /// Phantom data.
    marker: PhantomData<(T, M)>,
}

impl<S, T, M> ConvertRepo<S> for FileRepo<S, T, M>
where
    S: DataStore,
    T: SpecialType,
    M: FileMetadata,
{
    fn from_repo(mut repository: ObjectRepo<S>) -> crate::Result<Self> {
        if check_version(&mut repository, VERSION_ID)? {
            // Read and deserialize the table of entry paths.
            let mut object = repository
                .managed_object(TABLE_OBJECT_ID)
                .ok_or(crate::Error::Corrupt)?;
            let path_table = object.deserialize()?;

            Ok(Self {
                repository,
                path_table,
                marker: PhantomData,
            })
        } else {
            // Create and write the table of entry paths.
            let mut object = repository.add_managed(TABLE_OBJECT_ID);
            let path_table = PathTree::new();
            object.serialize(&path_table)?;
            drop(object);

            repository.commit()?;

            Ok(Self {
                repository,
                path_table,
                marker: PhantomData,
            })
        }
    }

    fn into_repo(mut self) -> crate::Result<ObjectRepo<S>> {
        self.repository.rollback()?;
        Ok(self.repository)
    }
}

impl<S, T, M> FileRepo<S, T, M>
where
    S: DataStore,
    T: SpecialType,
    M: FileMetadata,
{
    /// Return whether there is an entry at `path`.
    pub fn exists(&self, path: impl AsRef<RelativePath>) -> bool {
        self.path_table.contains(path.as_ref())
    }

    /// Return `true` if the given `path` has a parent directory in the repository.
    fn has_parent(&self, path: &RelativePath) -> bool {
        match path.parent() {
            Some(parent) if parent != *EMPTY_PARENT => match self.path_table.get(parent) {
                Some(handle) => matches!(handle.entry_type, EntryType::Directory),
                None => false,
            },
            _ => true,
        }
    }

    /// Add a new empty file or directory entry to the repository at the given `path`.
    ///
    /// # Examples
    /// Create a new regular file with no metadata.
    /// ```
    /// # use acid_store::repo::OpenOptions;
    /// # use acid_store::repo::file::{FileRepo, Entry, RelativePath};
    /// # use acid_store::store::MemoryStore;
    /// #
    /// # let mut repo = OpenOptions::new(MemoryStore::new())
    /// #    .create_new::<FileRepo<_>>()
    /// #    .unwrap();
    /// #
    /// let entry_path = RelativePath::new("file");
    /// repo.create(entry_path, &Entry::file()).unwrap();
    ///
    /// ```
    ///
    /// Create a new symbolic link with no metadata.
    /// ```
    /// # use std::path::Path;
    /// # use acid_store::repo::OpenOptions;
    /// # use acid_store::repo::file::{FileRepo, Entry, RelativePath, UnixSpecialType};
    /// # use acid_store::store::MemoryStore;
    /// #
    /// # let mut repo = OpenOptions::new(MemoryStore::new())
    /// #    .create_new::<FileRepo<_, UnixSpecialType>>()
    /// #    .unwrap();
    /// #
    /// let entry_path = RelativePath::new("link");
    /// let symbolic_link = UnixSpecialType::SymbolicLink {
    ///     target: Path::new("target").to_owned()
    /// };
    /// repo.create(entry_path, &Entry::special(symbolic_link)).unwrap();
    ///
    /// ```
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `path` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `path`.
    /// - `Error::Serialize`: The new file metadata could not be serialized.
    /// - `Error::Deserialize`: The old file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create(
        &mut self,
        path: impl AsRef<RelativePath>,
        entry: &Entry<T, M>,
    ) -> crate::Result<()> {
        if self.exists(&path) {
            return Err(crate::Error::AlreadyExists);
        }

        if !self.has_parent(path.as_ref()) {
            return Err(crate::Error::InvalidPath);
        }

        // Write the entry for the file.
        let mut entry_handle = self.repository.add_unmanaged();
        let mut object = self
            .repository
            .unmanaged_object_mut(&mut entry_handle)
            .unwrap();
        object.serialize(entry)?;
        drop(object);

        let file_handle = match entry.file_type {
            FileType::File => EntryType::File(self.repository.add_unmanaged()),
            FileType::Directory => EntryType::Directory,
            FileType::Special(_) => EntryType::Special,
        };

        let handle = EntryHandle {
            entry: entry_handle,
            entry_type: file_handle,
        };

        self.path_table.insert(path.as_ref(), handle);

        Ok(())
    }

    /// Add a new empty file or directory entry to the repository at the given `path`.
    ///
    /// This also creates any missing parent directories.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: There is already an entry at `path`.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Serialize`: The new file metadata could not be serialized.
    /// - `Error::Deserialize`: The old file metadata could not be deserialized.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create_parents(
        &mut self,
        path: impl AsRef<RelativePath>,
        entry: &Entry<T, M>,
    ) -> crate::Result<()> {
        let parent = match path.as_ref().parent() {
            Some(parent) if parent != *EMPTY_PARENT => parent,
            _ => return self.create(path, entry),
        };

        let mut ancestor = RelativePathBuf::new();
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
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotEmpty`: The entry is a directory which is not empty.
    pub fn remove(&mut self, path: impl AsRef<RelativePath>) -> crate::Result<()> {
        match self.path_table.list(&path) {
            Some(mut children) => {
                if children.next().is_some() {
                    return Err(crate::Error::NotEmpty);
                }
            }
            None => return Err(crate::Error::NotFound),
        }

        let entry_handle = self.path_table.remove(path.as_ref()).unwrap();
        if let EntryType::File(handle) = entry_handle.entry_type {
            self.repository.remove_unmanaged(&handle);
        }
        self.repository.remove_unmanaged(&entry_handle.entry);

        Ok(())
    }

    /// Remove the entry with the given `path` and its descendants from the repository.
    ///
    /// The space used by the given entry isn't freed and made available for new entries until
    /// `commit` is called.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    pub fn remove_tree(&mut self, path: impl AsRef<RelativePath>) -> crate::Result<()> {
        for (_, handles) in self
            .path_table
            .drain(path.as_ref())
            .ok_or(crate::Error::NotFound)?
        {
            if let EntryType::File(handle) = &handles.entry_type {
                self.repository.remove_unmanaged(&handle);
            }
            self.repository.remove_unmanaged(&handles.entry);
        }

        Ok(())
    }

    /// Return the entry at `path`.
    ///
    /// # Examples
    /// Check if an entry is a regular file.
    /// ```
    /// # use acid_store::repo::OpenOptions;
    /// # use acid_store::repo::file::{FileRepo, Entry, RelativePath};
    /// # use acid_store::store::MemoryStore;
    /// #
    /// # let mut repo = OpenOptions::new(MemoryStore::new())
    /// #    .create_new::<FileRepo<_>>()
    /// #    .unwrap();
    /// #
    /// let entry_path = RelativePath::new("file");
    /// repo.create(entry_path, &Entry::file()).unwrap();
    /// assert!(repo.entry(entry_path).unwrap().is_file())
    /// ```
    /// # Errors
    /// - `Error::NotFound`: There is no entry at `path`.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn entry(&self, path: impl AsRef<RelativePath>) -> crate::Result<Entry<T, M>> {
        let entry_handle = &self
            .path_table
            .get(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let mut object = self
            .repository
            .unmanaged_object(&entry_handle.entry)
            .unwrap();
        object.deserialize()
    }

    /// Set the file `metadata` for the entry at `path`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry at `path`.
    /// - `Error::Serialize`: The new file metadata could not be serialized.
    /// - `Error::Deserialize`: The old file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn set_metadata(
        &mut self,
        path: impl AsRef<RelativePath>,
        metadata: Option<M>,
    ) -> crate::Result<()> {
        let entry_handle = &mut self
            .path_table
            .get_mut(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let mut object = self
            .repository
            .unmanaged_object_mut(&mut entry_handle.entry)
            .unwrap();
        let mut entry: Entry<T, M> = object.deserialize()?;
        entry.metadata = metadata;
        object.serialize(&entry)
    }

    /// Return a `ReadOnlyObject` for reading the contents of the file at `path`.
    ///
    /// The returned object provides read-only access to the file. To get read-write access, use
    /// `open_mut`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotFile`: The entry does not represent a regular file.
    pub fn open(&self, path: impl AsRef<RelativePath>) -> crate::Result<ReadOnlyObject<S>> {
        let entry_handle = self
            .path_table
            .get(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        if let EntryType::File(handle) = &entry_handle.entry_type {
            Ok(self.repository.unmanaged_object(&handle).unwrap())
        } else {
            Err(crate::Error::NotFile)
        }
    }

    /// Return an `Object` for reading and writing the contents of the file at `path`.
    ///
    /// The returned object provides read-write access to the file. To get read-only access, use
    /// `open`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotFile`: The entry does not represent a regular file.
    pub fn open_mut(&mut self, path: impl AsRef<RelativePath>) -> crate::Result<Object<S>> {
        let entry_handle = self
            .path_table
            .get_mut(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        match entry_handle.entry_type {
            EntryType::File(ref mut handle) => {
                Ok(self.repository.unmanaged_object_mut(handle).unwrap())
            }
            _ => Err(crate::Error::NotFile),
        }
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
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    pub fn copy(
        &mut self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        if self.exists(dest.as_ref()) {
            return Err(crate::Error::AlreadyExists);
        }

        if !self.has_parent(dest.as_ref()) {
            return Err(crate::Error::InvalidPath);
        }

        let entry_handle = self
            .path_table
            .get(source.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let new_handle = EntryHandle {
            entry: self.repository.copy_unmanaged(&entry_handle.entry),
            entry_type: match &entry_handle.entry_type {
                EntryType::File(handle) => EntryType::File(self.repository.copy_unmanaged(&handle)),
                EntryType::Directory => EntryType::Directory,
                EntryType::Special => EntryType::Special,
            },
        };
        self.path_table.insert(dest.as_ref(), new_handle);

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
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    pub fn copy_tree(
        &mut self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        if self.exists(dest.as_ref()) {
            return Err(crate::Error::AlreadyExists);
        }

        if !self.has_parent(dest.as_ref()) {
            return Err(crate::Error::InvalidPath);
        }

        // Copy the root directory.
        let root_handle = self
            .path_table
            .get(source.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let new_root_handles = EntryHandle {
            entry: self.repository.copy_unmanaged(&root_handle.entry),
            entry_type: match &root_handle.entry_type {
                EntryType::File(handle) => EntryType::File(self.repository.copy_unmanaged(&handle)),
                EntryType::Directory => EntryType::Directory,
                EntryType::Special => EntryType::Special,
            },
        };
        let mut tree = PathTree::new();
        tree.insert(&dest, new_root_handles);

        // Because we can't walk the path tree and insert into it at the same time, we need to copy
        // the paths to a new `PathTree` first and then insert them back into the original.
        for (path, entry_handle) in self.path_table.walk(source.as_ref()).unwrap() {
            let relative_path = path.strip_prefix(&source).unwrap();
            let dest_path = dest.as_ref().join(relative_path);

            let new_handle = EntryHandle {
                entry: self.repository.copy_unmanaged(&entry_handle.entry),
                entry_type: match &entry_handle.entry_type {
                    EntryType::File(handle) => {
                        EntryType::File(self.repository.copy_unmanaged(&handle))
                    }
                    EntryType::Directory => EntryType::Directory,
                    EntryType::Special => EntryType::Special,
                },
            };

            tree.insert(dest_path, new_handle);
        }

        for (path, handles) in tree.drain(&dest).unwrap() {
            self.path_table.insert(path, handles);
        }

        Ok(())
    }

    /// Return an iterator of paths which are children of `parent`.
    ///
    /// # Errors
    /// - `Error::NotFound`: The given `parent` does not exist.
    /// - `Error::NotDirectory`: The given `parent` is not a directory.
    pub fn list<'a>(
        &'a self,
        parent: impl AsRef<RelativePath> + 'a,
    ) -> crate::Result<impl Iterator<Item = RelativePathBuf> + 'a> {
        let entry_handle = self
            .path_table
            .get(parent.as_ref())
            .ok_or(crate::Error::NotFound)?;
        if !matches!(entry_handle.entry_type, EntryType::Directory) {
            return Err(crate::Error::NotDirectory);
        }

        Ok(self.path_table.list(parent).unwrap().map(|(path, _)| path))
    }

    /// Return an iterator of paths which are descendants of `parent`.
    ///
    /// The returned iterator yields paths in depth-first order, meaning that a path will always
    /// come before its children.
    ///
    /// # Errors
    /// - `Error::NotFound`: The given `parent` does not exist.
    /// - `Error::NotDirectory`: The given `parent` is not a directory.
    pub fn walk<'a>(
        &'a self,
        parent: impl AsRef<RelativePath> + 'a,
    ) -> crate::Result<impl Iterator<Item = RelativePathBuf> + 'a> {
        let entry_handle = self
            .path_table
            .get(parent.as_ref())
            .ok_or(crate::Error::NotFound)?;
        if !matches!(entry_handle.entry_type, EntryType::Directory) {
            return Err(crate::Error::NotDirectory);
        }

        Ok(self.path_table.walk(parent).unwrap().map(|(path, _)| path))
    }

    /// Copy a file from the file system into the repository.
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// The `source` file's metadata will be copied to the `dest` entry according to the selected
    /// `FileMetadata` implementation.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::FileType`: The file at `source` is not a regular file, directory, or supported
    /// special file.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn archive(
        &mut self,
        source: impl AsRef<Path>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        if self.exists(&dest) {
            return Err(crate::Error::AlreadyExists);
        }

        let file_metadata = metadata(&source)?;

        let file_type = if file_metadata.is_file() {
            FileType::File
        } else if file_metadata.is_dir() {
            FileType::Directory
        } else {
            FileType::Special(T::from_file(source.as_ref())?.ok_or(crate::Error::FileType)?)
        };

        let entry = Entry {
            file_type,
            metadata: Some(M::from_file(source.as_ref())?),
        };

        self.create(&dest, &entry)?;

        // Write the contents of the file entry if it's a file.
        let entry_handle = self.path_table.get_mut(dest.as_ref()).unwrap();
        if let EntryType::File(ref mut handle) = entry_handle.entry_type {
            let mut object = self.repository.unmanaged_object_mut(handle).unwrap();
            let mut file = File::open(&source)?;
            copy(&mut file, &mut object)?;
            object.flush()?;
        }

        Ok(())
    }

    /// Copy a directory tree from the file system into the repository.
    ///
    /// If `source` is a directory, this also copies its descendants. If `source` is not a
    /// directory, this is the same as calling `archive`. If one of the files in the tree is not a
    /// regular file, directory, or supported special file, it is skipped.
    ///
    /// The `source` file's metadata will be copied to the `dest` entry according to the selected
    /// `FileMetadata` implementation.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn archive_tree(
        &mut self,
        source: impl AsRef<Path>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        // `WalkDir` includes `source` in the paths it iterates over.
        // It does not error if `source` is not a directory.
        let all_paths = WalkDir::new(&source).into_iter();

        for result in all_paths {
            let dir_entry = result.map_err(io::Error::from)?;
            let relative_path =
                RelativePath::from_path(dir_entry.path().strip_prefix(&source).unwrap())
                    .expect("Not a valid relative path.");
            match self.archive(dir_entry.path(), dest.as_ref().join(relative_path)) {
                Ok(_) => continue,
                Err(crate::Error::FileType) => continue,
                Err(error) => return Err(error),
            }
        }

        Ok(())
    }

    /// Copy an entry from the repository into the file system.
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// The `source` entry's metadata will be copied to the `dest` file according to the selected
    /// `FileMetadata` implementation.
    ///
    /// # Errors
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn extract(
        &self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<Path>,
    ) -> crate::Result<()> {
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
                let mut object = self.open(source.as_ref()).unwrap();
                let mut file = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&dest)?;
                copy(&mut object, &mut file)?;
            }
            FileType::Directory => {
                create_dir(&dest)?;
            }
            FileType::Special(special_type) => {
                special_type.create_file(dest.as_ref())?;
            }
        }

        // Set the file metadata.
        if let Some(metadata) = entry.metadata {
            metadata.write_metadata(dest.as_ref())?;
        }

        Ok(())
    }

    /// Copy a tree of entries from the repository into the file system.
    ///
    /// If `source` is a directory, this also copies its descendants. If `source` is not a
    /// directory, this is the same as calling `extract`.
    ///
    /// This accepts a `filter` which is passed the relative path of each entry in the tree. A file
    /// is only copied if `filter` returns `true`. A directory is not descended into unless `filter`
    /// returns `true`. To copy all files in the tree, pass `|_| true`.
    ///
    /// The `source` entry's metadata will be copied to the `dest` file according to the selected
    /// `FileMetadata` implementation.
    ///
    /// # Errors
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn extract_tree(
        &self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<Path>,
    ) -> crate::Result<()> {
        let relative_descendants = self
            .path_table
            .walk(&source)
            .ok_or(crate::Error::NotFound)?
            .map(|(path, _)| path.strip_prefix(&source).unwrap().to_owned());

        // Extract the root directory.
        self.extract(&source, &dest)?;

        // Extract the descendants.
        for descendant in relative_descendants {
            self.extract(
                source.as_ref().join(&descendant),
                descendant.to_path(dest.as_ref()),
            )?;
        }

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepo::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Serialize and write the tree of entry paths.
        let mut object = self.repository.add_managed(TABLE_OBJECT_ID);
        object.serialize(&self.path_table)?;
        drop(object);

        // Commit the underlying repository.
        self.repository.commit()
    }

    /// Roll back all changes made since the last commit.
    ///
    /// See `ObjectRepo::rollback` for details.
    pub fn rollback(&mut self) -> crate::Result<()> {
        // Read and deserialize the path table from the previous commit.
        let mut object = self
            .repository
            .managed_object(TABLE_OBJECT_ID)
            .ok_or(crate::Error::Corrupt)?;
        let path_table = match object.deserialize() {
            Err(crate::Error::Deserialize) => return Err(crate::Error::Corrupt),
            Err(error) => return Err(error),
            Ok(value) => value,
        };
        drop(object);

        self.repository.rollback()?;

        self.path_table = path_table;

        Ok(())
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// See `ObjectRepo::clean` for details.
    pub fn clean(&self) -> crate::Result<()> {
        self.repository.clean()
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of paths of files with corrupt data or metadata.
    ///
    /// If you just need to verify the integrity of one object, `Object::verify` is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&self) -> crate::Result<HashSet<RelativePathBuf>> {
        let report = self.repository.verify()?;

        // Check for corrupt metadata.
        Ok(self
            .path_table
            .walk(RelativePathBuf::new())
            .unwrap()
            .filter(|(_, entry_handle)| {
                let entry_valid = report.check_unmanaged(&entry_handle.entry);
                let file_valid = match &entry_handle.entry_type {
                    EntryType::File(handle) => report.check_unmanaged(&handle),
                    _ => true,
                };
                !entry_valid || !file_valid
            })
            .map(|(path, _)| path)
            .collect::<HashSet<_>>())
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepo::change_password` for details.
    #[cfg(feature = "encryption")]
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repository.change_password(new_password);
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> Uuid {
        self.repository.instance()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.repository.info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// See `ObjectRepo::peek_info` for details.
    pub fn peek_info(store: &mut S) -> crate::Result<RepoInfo> {
        ObjectRepo::peek_info(store)
    }
}
