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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::{create_dir, create_dir_all, hard_link, metadata};
use std::io;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use hex_literal::hex;
use once_cell::sync::Lazy;
use relative_path::{RelativePath, RelativePathBuf};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use walkdir::WalkDir;

use crate::repo::{
    key::KeyRepo, state::StateRepo, Commit, InstanceId, Object, OpenRepo, RepoInfo, RepoStats,
    ResourceLimit, RestoreSavepoint, Savepoint, Unlock, VersionId,
};

use super::entry::{Entry, EntryHandle, EntryType, HandleType};
use super::file::{archive_file, extract_file};
use super::iter::{Children, Descendants, WalkEntry, WalkPredicate};
use super::metadata::{FileMetadata, NoMetadata};
use super::path_tree::PathTree;
use super::special::{NoSpecial, SpecialType};
use crate::repo::file::entry::EntryId;
#[cfg(all(any(unix, doc), feature = "fuse-mount"))]
use {
    super::fuse::FuseAdapter, super::metadata::UnixMetadata, super::special::UnixSpecial,
    std::ffi::OsStr,
};

/// The path of the root entry.
pub static EMPTY_PATH: Lazy<RelativePathBuf> = Lazy::new(|| RelativePath::new("").to_owned());

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoState {
    /// The tree of entry paths and their handles.
    pub tree: PathTree<EntryHandle>,

    /// A map of entry IDs to their reference counts.
    pub links: HashMap<EntryId, u32>,
}

impl Default for RepoState {
    fn default() -> Self {
        Self {
            tree: PathTree::new(),
            links: HashMap::new(),
        }
    }
}

/// A virtual file system.
///
/// See [`crate::repo::file`] for more information.
#[derive(Debug)]
pub struct FileRepo<S = NoSpecial, M = NoMetadata>
where
    S: SpecialType,
    M: FileMetadata,
{
    pub(super) repo: StateRepo<RepoState>,
    marker: PhantomData<(S, M)>,
}

impl<S, M> OpenRepo for FileRepo<S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    type Key = <StateRepo<RepoState> as OpenRepo>::Key;

    const VERSION_ID: VersionId = VersionId::new(Uuid::from_bytes(hex!(
        "57ac9d00 fde6 11eb 82cd 1f2bdd384d98"
    )));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            repo: StateRepo::open_repo(repo)?,
            marker: PhantomData,
        })
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            repo: StateRepo::create_repo(repo)?,
            marker: PhantomData,
        })
    }

    fn into_repo(self) -> crate::Result<KeyRepo<Self::Key>> {
        self.repo.into_repo()
    }
}

impl<S, M> FileRepo<S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    /// Return whether there is an entry at `path`.
    pub fn exists(&self, path: impl AsRef<RelativePath>) -> bool {
        self.repo.state().tree.contains(path.as_ref())
    }

    /// Return whether the given `path` is a regular file entry.
    ///
    /// If there is no entry at `path`, this returns `false`.
    pub fn is_file(&self, path: impl AsRef<RelativePath>) -> bool {
        match self.repo.state().tree.get(path.as_ref()) {
            Some(entry) => matches!(entry.kind, HandleType::File(_)),
            None => false,
        }
    }

    /// Return whether the given `path` is a directory entry.
    ///
    /// If there is no entry at `path`, this returns `false`.
    pub fn is_directory(&self, path: impl AsRef<RelativePath>) -> bool {
        match self.repo.state().tree.get(path.as_ref()) {
            Some(entry) => matches!(entry.kind, HandleType::Directory),
            None => false,
        }
    }

    /// Return whether the given `path` is a special file entry.
    ///
    /// If there is no entry at `path`, this returns `false`.
    pub fn is_special(&self, path: impl AsRef<RelativePath>) -> bool {
        match self.repo.state().tree.get(path.as_ref()) {
            Some(entry) => matches!(entry.kind, HandleType::Special),
            None => false,
        }
    }

    /// Validate that the parent of the given `path` exists and is a directory.
    ///
    /// If the `path` is a root, this returns `Ok`.
    fn validate_parent(&self, path: &RelativePath) -> crate::Result<()> {
        match path.parent() {
            // This path is a root.
            Some(parent) if parent == *EMPTY_PATH => Ok(()),
            // This path has a parent segment.
            Some(parent) => match self.repo.state().tree.get(parent) {
                Some(handle) => match handle.kind {
                    HandleType::File(_) | HandleType::Special => Err(crate::Error::NotDirectory),
                    HandleType::Directory => Ok(()),
                },
                None => Err(crate::Error::NotFound),
            },
            // This path is empty.
            None => Err(crate::Error::InvalidPath),
        }
    }

    /// Add a new empty file or directory entry to the repository at the given `path`.
    ///
    /// # Examples
    /// Create a new regular file with no metadata.
    /// ```
    /// # use acid_store::repo::{OpenOptions, OpenMode};
    /// # use acid_store::repo::file::{FileRepo, Entry, RelativePath};
    /// # use acid_store::store::{MemoryStore, MemoryConfig};
    /// #
    /// # let mut repo: FileRepo = OpenOptions::new()
    /// #    .mode(OpenMode::CreateNew)
    /// #    .open(&MemoryConfig::new())
    /// #    .unwrap();
    /// #
    /// let entry_path = RelativePath::new("file");
    /// repo.create(entry_path, &Entry::file()).unwrap();
    ///
    /// ```
    ///
    /// Create a new symbolic link with no metadata.
    /// ```
    /// # #[cfg(feature = "file-metadata")] {
    /// # use std::path::Path;
    /// # use acid_store::repo::{OpenOptions, OpenMode};
    /// # use acid_store::repo::file::{FileRepo, Entry, RelativePath, UnixSpecial};
    /// # use acid_store::store::{MemoryStore, MemoryConfig};
    /// #
    /// # let mut repo: FileRepo<UnixSpecial> = OpenOptions::new()
    /// #    .mode(OpenMode::CreateNew)
    /// #    .open(&MemoryConfig::new())
    /// #    .unwrap();
    /// #
    /// let entry_path = RelativePath::new("link");
    /// let symbolic_link = UnixSpecial::Symlink {
    ///     target: Path::new("target").to_owned()
    /// };
    /// repo.create(entry_path, &Entry::special(symbolic_link)).unwrap();
    /// # }
    /// ```
    ///
    /// # Errors
    /// - `Error::NotFound`: The parent of `path` does not exist.
    /// - `Error::NotDirectory`: The parent of `path` is not a directory entry.
    /// - `Error::InvalidPath`: The given `path` is empty.
    /// - `Error::AlreadyExists`: There is already an entry at `path`.
    /// - `Error::Serialize`: The new file metadata could not be serialized.
    /// - `Error::Deserialize`: The old file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create(
        &mut self,
        path: impl AsRef<RelativePath>,
        entry: &Entry<S, M>,
    ) -> crate::Result<()> {
        self.validate_parent(path.as_ref())?;

        if self.exists(&path) {
            return Err(crate::Error::AlreadyExists);
        }

        let entry_key = self.repo.create();
        let mut object = self.repo.object(entry_key).unwrap();
        let result = object.serialize(entry);
        drop(object);
        if let Err(error) = result {
            self.repo.remove(entry_key);
            return Err(error);
        }

        let entry_type = match entry.kind {
            EntryType::File => HandleType::File(self.repo.create()),
            EntryType::Directory => HandleType::Directory,
            EntryType::Special(_) => HandleType::Special,
        };

        let handle = EntryHandle {
            entry: entry_key,
            kind: entry_type,
        };

        self.repo.state_mut().links.insert(handle.id(), 1);
        self.repo.state_mut().tree.insert(path.as_ref(), handle);

        Ok(())
    }

    /// Add a new empty file or directory entry to the repository at the given `path`.
    ///
    /// This also creates any missing parent directories.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is empty.
    /// - `Error::AlreadyExists`: There is already an entry at `path`.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Serialize`: The new file metadata could not be serialized.
    /// - `Error::Deserialize`: The old file metadata could not be deserialized.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create_parents(
        &mut self,
        path: impl AsRef<RelativePath>,
        entry: &Entry<S, M>,
    ) -> crate::Result<()> {
        let parent = match path.as_ref().parent() {
            Some(parent) if parent != *EMPTY_PATH => parent,
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

    /// Remove the given `handle` from the repository.
    fn remove_handle(&mut self, handle: EntryHandle) {
        let num_links = {
            let num_links = self.repo.state_mut().links.get_mut(&handle.id()).unwrap();
            *num_links -= 1;
            *num_links
        };

        if num_links == 0 {
            if let HandleType::File(object_id) = handle.kind {
                self.repo.remove(object_id);
            }
            self.repo.remove(handle.entry);
            self.repo.state_mut().links.remove(&handle.id());
        }
    }

    /// Remove the entry with the given `path` from the repository.
    ///
    /// The space used by the given entry isn't reclaimed in the backing data store until changes
    /// are committed and [`Commit::clean`] is called.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is empty.
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotEmpty`: The entry is a directory which is not empty.
    ///
    /// [`Commit::clean`]: crate::repo::Commit::clean
    pub fn remove(&mut self, path: impl AsRef<RelativePath>) -> crate::Result<()> {
        if path.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        match self.repo.state().tree.children(&path) {
            Some(mut children) => {
                if children.next().is_some() {
                    return Err(crate::Error::NotEmpty);
                }
            }
            None => return Err(crate::Error::NotFound),
        }

        let entry_handle = self.repo.state_mut().tree.remove(path.as_ref()).unwrap();

        self.remove_handle(entry_handle);

        Ok(())
    }

    /// Remove the entry with the given `path` and its descendants from the repository.
    ///
    /// The space used by the given entry isn't reclaimed in the backing data store until changes
    /// are committed and [`Commit::clean`] is called.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is empty.
    /// - `Error::NotFound`: There is no entry with the given `path`.
    ///
    /// [`clean`]: crate::repo::Commit::clean
    pub fn remove_tree(&mut self, path: impl AsRef<RelativePath>) -> crate::Result<()> {
        if path.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        let handles = self
            .repo
            .state_mut()
            .tree
            .drain(path.as_ref())
            .ok_or(crate::Error::NotFound)?
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();

        for handle in handles {
            self.remove_handle(handle);
        }

        Ok(())
    }

    /// Return the entry at `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is empty.
    /// - `Error::NotFound`: There is no entry at `path`.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn entry(&self, path: impl AsRef<RelativePath>) -> crate::Result<Entry<S, M>> {
        if path.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }
        let entry_handle = &self
            .repo
            .state()
            .tree
            .get(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let mut object = self.repo.object(entry_handle.entry).unwrap();
        object.deserialize()
    }

    /// Return the `EntryId` of the entry at `path`.
    ///
    /// An `EntryId` can be used to determine if two paths refer to the same entry, which happens
    /// when entries are linked using [`link`].
    ///
    /// # Examples
    /// ```
    /// # use acid_store::repo::{OpenOptions, OpenMode};
    /// # use acid_store::repo::file::{Entry, FileRepo};
    /// # use acid_store::store::MemoryConfig;
    /// #
    /// # let mut repo: FileRepo = OpenOptions::new()
    /// #    .mode(OpenMode::CreateNew)
    /// #    .open(&MemoryConfig::new())
    /// #    .unwrap();
    /// repo.create("one", &Entry::file()).unwrap();
    /// repo.link("one", "two").unwrap();
    ///
    /// assert_eq!(repo.entry_id("one").unwrap(), repo.entry_id("two").unwrap());
    /// ```
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is empty.
    /// - `Error::NotFound`: There is no entry at `path`.
    ///
    /// [`link`]: crate::repo::file::FileRepo::link
    pub fn entry_id(&self, path: impl AsRef<RelativePath>) -> crate::Result<EntryId> {
        if path.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        let entry_handle = &self
            .repo
            .state()
            .tree
            .get(path.as_ref())
            .ok_or(crate::Error::NotFound)?;

        Ok(entry_handle.id())
    }

    /// Set the file `metadata` for the entry at `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is empty.
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
        if path.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        let entry_handle = *self
            .repo
            .state()
            .tree
            .get(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let mut object = self.repo.object(entry_handle.entry).unwrap();
        let mut entry: Entry<S, M> = object.deserialize()?;
        entry.metadata = metadata;
        object.serialize(&entry)
    }

    /// Return an `Object` for reading and writing the contents of the file at `path`.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `path` is empty.
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotFile`: The entry does not represent a regular file.
    pub fn open(&self, path: impl AsRef<RelativePath>) -> crate::Result<Object> {
        if path.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        let entry_handle = *self
            .repo
            .state()
            .tree
            .get(path.as_ref())
            .ok_or(crate::Error::NotFound)?;

        if let HandleType::File(object_id) = entry_handle.kind {
            Ok(self.repo.object(object_id).unwrap())
        } else {
            Err(crate::Error::NotFile)
        }
    }

    /// Create and return a copy of the given `EntryHandle`.
    fn copy_entry_handle(&mut self, handle: EntryHandle) -> EntryHandle {
        let new_entry_key = self.repo.copy(handle.entry).unwrap();
        let handle = EntryHandle {
            entry: new_entry_key,
            kind: match handle.kind {
                HandleType::File(file_id) => HandleType::File(self.repo.copy(file_id).unwrap()),
                HandleType::Directory => HandleType::Directory,
                HandleType::Special => HandleType::Special,
            },
        };
        self.repo.state_mut().links.insert(handle.id(), 1);
        handle
    }

    /// Copy the entry at `source` to `dest`.
    ///
    /// If `source` is a directory entry, its descendants are not copied.
    ///
    /// This copies the entry from one location in the repository to another. To copy files from the
    /// file system to the repository, see [`archive`]. To copy files from the repository to the
    /// file system, see [`extract`].
    ///
    /// This is a cheap operation which does not require copying the bytes in the files.
    ///
    /// # Errors
    /// - `Error::NotFound`: The parent of `dest` does not exist.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::NotDirectory`: The parent of `dest` is not a directory entry.
    /// - `Error::InvalidPath`: The given `source` or `dest` paths are empty.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    ///
    /// [`archive`]: crate::repo::file::FileRepo::archive
    /// [`extract`]: crate::repo::file::FileRepo::extract
    pub fn copy(
        &mut self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        if source.as_ref() == *EMPTY_PATH || dest.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        self.validate_parent(dest.as_ref())?;

        if self.exists(dest.as_ref()) {
            return Err(crate::Error::AlreadyExists);
        }

        let entry_handle = *self
            .repo
            .state()
            .tree
            .get(source.as_ref())
            .ok_or(crate::Error::NotFound)?;

        let new_handle = self.copy_entry_handle(entry_handle);
        self.repo.state_mut().tree.insert(dest.as_ref(), new_handle);

        Ok(())
    }

    /// Copy the tree of entries at `source` to `dest`.
    ///
    /// If `source` is a directory entry, this also copies its descendants.
    ///
    /// This copies entries from one location in the repository to another. To copy files from the
    /// file system to the repository, see [`archive_tree`]. To copy files from the repository to
    /// the file system, see [`extract_tree`].
    ///
    /// This is a cheap operation which does not require copying the bytes in the files.
    ///
    /// # Errors
    /// - `Error::NotFound`: The parent of `dest` does not exist.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::NotDirectory`: The parent of `dest` is not a directory entry.
    /// - `Error::InvalidPath`: The given `source` or `dest` paths are empty.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    ///
    /// [`archive_tree`]: crate::repo::file::FileRepo::archive
    /// [`extract_tree`]: crate::repo::file::FileRepo::extract
    pub fn copy_tree(
        &mut self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        if source.as_ref() == *EMPTY_PATH || dest.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        self.validate_parent(dest.as_ref())?;

        if self.exists(dest.as_ref()) {
            return Err(crate::Error::AlreadyExists);
        }

        // Copy the root path.
        let source_root_handle = *self
            .repo
            .state()
            .tree
            .get(source.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let dest_root_handle = self.copy_entry_handle(source_root_handle);
        self.repo
            .state_mut()
            .tree
            .insert(dest.as_ref(), dest_root_handle);

        // Because we can't walk the path tree and insert into it at the same time, we need to
        // construct a tree of the destination paths before inserting them back into the path table.
        // Using this prefix tree type should consume less memory than collecting the paths into a
        // vector.
        let mut dest_tree = PathTree::new();

        // Create an arbitrary root entry in the destination tree. The name of this root entry isn't
        // significant.
        let dest_tree_root = RelativePath::new("dest_rot");
        dest_tree.insert(dest_tree_root, source_root_handle);

        // Get the destination paths for each path in the path table and insert them into the
        // destination tree.
        for (path, source_handle) in self.repo.state().tree.descendants(source.as_ref()).unwrap() {
            let relative_path = path.strip_prefix(&source).unwrap();
            let dest_tree_path = dest_tree_root.join(relative_path);
            dest_tree.insert(dest_tree_path, *source_handle);
        }

        // Move the rest of the paths from the destination tree into the path table.
        for (dest_tree_path, source_handle) in dest_tree.drain(dest_tree_root).unwrap() {
            let dest_handle = self.copy_entry_handle(source_handle);
            let relative_path = dest_tree_path.strip_prefix(dest_tree_root).unwrap();
            let dest_path = dest.as_ref().join(relative_path);
            self.repo.state_mut().tree.insert(&dest_path, dest_handle);
        }

        Ok(())
    }

    /// Move the tree of entries at `source` to `dest`.
    ///
    /// If `source` is a directory entry, this also moves its descendants.
    ///
    /// This method is different from calling [`copy_tree`] followed by [`remove_tree`] on the
    /// `source` tree in how it handles links. While [`copy_tree`] creates a new entry with the same
    /// metadata and contents, this method moves the original entry, so links created with [`link`]
    /// are preserved and the entries in the `dest` tree will have the same [`EntryId`] as the
    /// entries in the `source` tree.
    ///
    /// This is a cheap operation which does not require copying the bytes in the files.
    ///
    /// # Errors
    /// - `Error::NotFound`: The parent of `dest` does not exist.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::NotDirectory`: The parent of `dest` is not a directory entry.
    /// - `Error::InvalidPath`: The given `source` or `dest` paths are empty.
    /// - `Error::InvalidPath`: The given `dest` is a descendant of `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    ///
    /// [`copy_tree`]: crate::repo::file::FileRepo::copy_tree
    /// [`remove_tree`]: crate::repo::file::FileRepo::remove_tree
    /// [`link`]: crate::repo::file::FileRepo::link
    /// [`EntryId`]: crate::repo::file::EntryId
    pub fn rename(
        &mut self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        if source.as_ref() == *EMPTY_PATH || dest.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        if dest.as_ref().starts_with(source.as_ref()) {
            return Err(crate::Error::InvalidPath);
        }

        self.validate_parent(dest.as_ref())?;

        if self.exists(dest.as_ref()) {
            return Err(crate::Error::AlreadyExists);
        }

        let source_tree = self
            .repo
            .state_mut()
            .tree
            .drain(source.as_ref())
            .ok_or(crate::Error::NotFound)?
            .collect::<Vec<_>>();

        for (source_path, handle) in source_tree.into_iter() {
            let relative_path = source_path.strip_prefix(source.as_ref()).unwrap();
            let dest_path = dest.as_ref().join(relative_path);
            self.repo.state_mut().tree.insert(dest_path, handle);
        }

        Ok(())
    }

    /// Create a link to the `source` entry at `dest`.
    ///
    /// Linked entries share the same metadata and the same contents, so changes to `source` are
    /// reflected in `dest` and vice versa. This is different from creating a copy of an entry using
    /// [`copy`]. Linking entries is analogous to creating a hard link in a file system. It is not
    /// possible to link directory entries.
    ///
    /// You can use [`entry_id`] to determine if two paths refer to the same entry. You can use
    /// [`link_count`] to determine the number of links there are to an entry.
    ///
    /// # Examples
    /// ```
    /// # use std::io::{Read, Write};
    /// # use acid_store::repo::{OpenOptions, OpenMode};
    /// # use acid_store::repo::file::{Entry, FileRepo};
    /// # use acid_store::store::MemoryConfig;
    /// #
    /// # let mut repo: FileRepo = OpenOptions::new()
    /// #    .mode(OpenMode::CreateNew)
    /// #    .open(&MemoryConfig::new())
    /// #    .unwrap();
    /// repo.create("one", &Entry::file()).unwrap();
    /// repo.link("one", "two").unwrap();
    ///
    /// let mut object = repo.open("two").unwrap();
    /// object.write_all(b"test data").unwrap();
    /// object.commit();
    ///
    /// let mut object = repo.open("one").unwrap();
    /// let mut contents = Vec::new();
    /// object.read_to_end(&mut contents).unwrap();
    ///
    /// assert_eq!(&contents, b"test data");
    /// ```
    ///
    /// # Errors
    /// - `Error::NotFile`: The `source` entry is a directory entry.
    /// - `Error::NotFound`: The parent of `dest` does not exist.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::NotDirectory`: The parent of `dest` is not a directory entry.
    /// - `Error::InvalidPath`: The given `source` or `dest` paths are empty.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    ///
    /// [`copy`]: crate::repo::file::FileRepo::copy
    /// [`entry_id`]: crate::repo::file::FileRepo::entry_id
    /// [`link_count`]: crate::repo::file::FileRepo::link_count
    pub fn link(
        &mut self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        if source.as_ref() == *EMPTY_PATH || dest.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        self.validate_parent(dest.as_ref())?;

        if self.exists(dest.as_ref()) {
            return Err(crate::Error::AlreadyExists);
        }

        let entry_handle = *self
            .repo
            .state()
            .tree
            .get(source.as_ref())
            .ok_or(crate::Error::NotFound)?;

        if matches!(entry_handle.kind, HandleType::Directory) {
            return Err(crate::Error::NotFile);
        }

        self.repo
            .state_mut()
            .tree
            .insert(dest.as_ref(), entry_handle);

        *self
            .repo
            .state_mut()
            .links
            .get_mut(&entry_handle.id())
            .unwrap() += 1;

        Ok(())
    }

    /// The number of links to the entry with the given `id`.
    ///
    /// This returns the number of paths which refer to the entry with the given `id`. This number
    /// can be greater than 1 if entries have been linked using [`link`]. If there is no entry with
    /// the given `id` in the repository, this returns 0.
    ///
    /// [`link`]: crate::repo::file::FileRepo::link
    pub fn link_count(&self, id: EntryId) -> u32 {
        self.repo.state().links.get(&id).copied().unwrap_or(0)
    }

    /// Verify that `path` has descendants.
    fn verify_has_descendants(&self, parent: &RelativePath) -> crate::Result<()> {
        if parent == *EMPTY_PATH {
            return Ok(());
        }

        let entry_handle = self
            .repo
            .state()
            .tree
            .get(parent.as_ref())
            .ok_or(crate::Error::NotFound)?;

        if !matches!(entry_handle.kind, HandleType::Directory) {
            return Err(crate::Error::NotDirectory);
        }

        Ok(())
    }

    /// Return an iterator of paths which are immediate children of `parent`.
    ///
    /// The given `parent` may be an empty path, in which case the paths of top-level entries are
    /// returned.
    ///
    /// The `parent` path is not included in the returned iterator.
    ///
    /// # Errors
    /// - `Error::NotFound`: The given `parent` does not exist.
    /// - `Error::NotDirectory`: The given `parent` is not a directory.
    pub fn children<'a>(
        &'a self,
        parent: impl AsRef<RelativePath> + 'a,
    ) -> crate::Result<Children<'a>> {
        self.verify_has_descendants(parent.as_ref())?;
        Ok(Children(self.repo.state().tree.children(parent).unwrap()))
    }

    /// Return an iterator of paths which are descendants of `parent`.
    ///
    /// The given `parent` may be an empty path, in which case the paths of all entries in the
    /// repository are returned.
    ///
    /// The `parent` path is not included in the returned iterator.
    ///
    /// The returned iterator yields paths in depth-first order, meaning that a path will always
    /// come before its children.
    ///
    /// # Errors
    /// - `Error::NotFound`: The given `parent` does not exist.
    /// - `Error::NotDirectory`: The given `parent` is not a directory.
    pub fn descendants<'a>(
        &'a self,
        parent: impl AsRef<RelativePath> + 'a,
    ) -> crate::Result<Descendants<'a>> {
        self.verify_has_descendants(parent.as_ref())?;
        Ok(Descendants(
            self.repo.state().tree.descendants(parent).unwrap(),
        ))
    }

    /// Walk through the tree of descendants of `parent`.
    ///
    /// This method accepts a `visitor` which is passed a `WalkEntry` for each entry in the tree and
    /// returns a `WalkPredicate`. If the `visitor` returns `WalkPredicate::Stop` at any point, this
    /// method returns early with the wrapped value. Otherwise, this method returns `Ok(None)`.
    ///
    /// The given `parent` may be an empty path, in which case all entries in the repository are
    /// visited
    ///
    /// The `parent` path is not visited.
    ///
    /// This method visits entries in depth-first order, meaning that an entry will always be
    /// visited before its children.
    ///
    /// # Examples
    /// Find all the paths of empty file entries in the repository.
    /// ```no_run
    /// # use acid_store::repo::{OpenOptions, OpenMode};
    /// # use acid_store::repo::file::{WalkPredicate, FileRepo};
    /// # use acid_store::store::MemoryConfig;
    /// #
    /// # let repo: FileRepo = OpenOptions::new()
    /// #    .mode(OpenMode::CreateNew)
    /// #    .open(&MemoryConfig::new())
    /// #    .unwrap();
    /// #
    /// let mut empty_files = Vec::new();
    /// repo.walk::<(), _, _>("", |entry| {
    ///     if let Some(object) = entry.open() {
    ///         if object.size().unwrap() == 0 {
    ///             empty_files.push(entry.into_path());
    ///         }
    ///     }
    ///     WalkPredicate::Continue
    /// });
    /// ```
    ///
    /// Search two directory trees for the path of a symbolic link with a given target.
    /// ```no_run
    /// # use acid_store::repo::{OpenOptions, OpenMode};
    /// # use acid_store::repo::file::{UnixSpecial, EntryType, WalkPredicate, FileRepo};
    /// # use acid_store::store::MemoryConfig;
    /// #
    /// # let repo: FileRepo = OpenOptions::new()
    /// #    .mode(OpenMode::CreateNew)
    /// #    .open(&MemoryConfig::new())
    /// #    .unwrap();
    /// #
    /// let symlink = UnixSpecial::Symlink { target: "/home/lostatc/target".into() };
    /// let mut result = repo.walk("", |entry| {
    ///     if !entry.path().starts_with("first") && !entry.path().starts_with("second") {
    ///         return WalkPredicate::SkipDescendants;
    ///     }
    ///
    ///     match entry.entry().map(|entry| entry.kind) {
    ///         Ok(EntryType::Special(symlink)) => WalkPredicate::Stop(Ok(entry.into_path())),
    ///         Err(error) => WalkPredicate::Stop(Err(error)),
    ///         _ => WalkPredicate::Continue,
    ///     }
    /// });
    /// ```
    ///
    /// # Errors
    /// - `Error::NotFound`: The given `parent` does not exist.
    /// - `Error::NotDirectory`: The given `parent` is not a directory.
    pub fn walk<R, P, F>(&self, parent: P, mut visitor: F) -> crate::Result<Option<R>>
    where
        P: AsRef<RelativePath>,
        F: FnMut(WalkEntry<S, M>) -> WalkPredicate<R>,
    {
        self.verify_has_descendants(parent.as_ref())?;

        Ok(self
            .repo
            .state()
            .tree
            .walk(parent.as_ref(), |walk_entry| {
                visitor(WalkEntry {
                    path: walk_entry.path,
                    base: parent.as_ref(),
                    handle: *walk_entry.value,
                    depth: walk_entry.depth,
                    repo: &self,
                })
            })
            .unwrap())
    }

    /// Copy a file from the file system into the repository.
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// The `source` file's metadata will be copied to the `dest` entry according to the selected
    /// [`FileMetadata`] implementation.
    ///
    /// If `source` is a sparse file, this method will attempt to efficiently copy any sparse holes
    /// in the file to the [`Object`] in the repository, creating a sparse object.
    ///
    /// # Errors
    /// - `Error::NotFound`: The parent of `dest` does not exist.
    /// - `Error::NotDirectory`: The parent of `dest` is not a directory entry.
    /// - `Error::InvalidPath`: The given `dest` path is empty.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::FileType`: The file at `source` is not a regular file, directory, or supported
    /// special file.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`FileMetadata`]: crate::repo::file::FileMetadata
    /// [`Object`]: crate::repo::Object
    pub fn archive(
        &mut self,
        source: impl AsRef<Path>,
        dest: impl AsRef<RelativePath>,
    ) -> crate::Result<()> {
        if dest.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        if self.exists(&dest) {
            return Err(crate::Error::AlreadyExists);
        }

        let file_metadata = metadata(&source)?;

        let file_type = if file_metadata.is_file() {
            EntryType::File
        } else if file_metadata.is_dir() {
            EntryType::Directory
        } else {
            EntryType::Special(S::from_file(source.as_ref())?.ok_or(crate::Error::FileType)?)
        };

        let entry = Entry {
            kind: file_type,
            metadata: M::from_file(source.as_ref())?,
        };

        self.create(&dest, &entry)?;

        // Write the contents of the file entry if it's a file.
        let entry_handle = self.repo.state().tree.get(dest.as_ref()).unwrap();
        if let HandleType::File(object_id) = entry_handle.kind {
            let mut object = self.repo.object(object_id).unwrap();
            archive_file(&mut object, source.as_ref())?;
        }

        Ok(())
    }

    /// Copy a directory tree from the file system into the repository.
    ///
    /// If `source` is a directory, this also copies its descendants. If `source` is not a
    /// directory, this is the same as calling [`archive`]. If one of the files in the tree is not a
    /// regular file, directory, or supported special file, it is skipped.
    ///
    /// The metadata of the files in the `source` tree will be copied to the `dest` entries
    /// according to the selected [`FileMetadata`] implementation.
    ///
    /// If a file in the `source` tree is a sparse file, this method will attempt to efficiently
    /// copy any sparse holes in the file to the [`Object`] in the repository, creating a sparse
    /// object.
    ///
    /// This method does not attempt to handle hard links in the `source` tree specially; if two
    /// files in the `source` tree are hard links, they will be archived as separate entries.
    ///
    /// # Errors
    /// - `Error::NotFound`: The parent of `dest` does not exist.
    /// - `Error::NotDirectory`: The parent of `dest` is not a directory entry.
    /// - `Error::InvalidPath`: The given `dest` path is empty.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`archive`]: crate::repo::file::FileRepo::archive
    /// [`FileMetadata`]: crate::repo::file::FileMetadata
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
    /// [`FileMetadata`] implementation.
    ///
    /// If `source` is a sparse object, this method will attempt to efficiently copy any sparse
    /// holes in the [`Object`] to the file in the file system, creating a sparse file.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `source` path is empty.
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`FileMetadata`]: crate::repo::file::FileMetadata
    /// [`Object`]: crate::repo::Object
    pub fn extract(
        &self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<Path>,
    ) -> crate::Result<()> {
        if source.as_ref() == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        if dest.as_ref().exists() {
            return Err(crate::Error::AlreadyExists);
        }

        let entry = self.entry(&source)?;

        // Create any necessary parent directories.
        if let Some(parent) = dest.as_ref().parent() {
            create_dir_all(parent)?
        }

        // Create the file or directory.
        match entry.kind {
            EntryType::File => {
                let mut object = self.open(source.as_ref()).unwrap();
                extract_file(&mut object, dest.as_ref())?;
            }
            EntryType::Directory => {
                create_dir(&dest)?;
            }
            EntryType::Special(special_type) => {
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
    /// directory, this is the same as calling [`extract`].
    ///
    /// The metadata of the entries in the `source` tree will be copied to the `dest` files
    /// according to the selected [`FileMetadata`] implementation.
    ///
    /// If an entry in the `source` tree is a sparse object, this method will attempt to efficiently
    /// copy any sparse holes in the [`Object`] to the file in the file system, creating a sparse
    /// file.
    ///
    /// If entries in the `source` tree are linked via [`link`], this method will attempt to make
    /// them hard links in the file system.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `source` path is empty.
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`extract`]: crate::repo::file::FileRepo::extract
    /// [`FileMetadata`]: crate::repo::file::FileMetadata
    /// [`Object`]: crate::repo::Object
    /// [`link`]: crate::repo::file::FileRepo::link
    pub fn extract_tree(
        &self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<Path>,
    ) -> crate::Result<()> {
        self.extract(&source, &dest)?;

        let mut link_map: HashMap<EntryId, PathBuf> = HashMap::new();

        let walk_result: crate::Result<Option<crate::Error>> = self.walk(&source, |entry| {
            let relative_path = entry.path().strip_prefix(&source).unwrap();
            let dest_path = relative_path.to_path(dest.as_ref());

            match link_map.get(&entry.entry_id()) {
                Some(original_path) => {
                    if let Err(error) = hard_link(original_path, &dest_path) {
                        return WalkPredicate::Stop(error.into());
                    }
                }
                None => {
                    if let Err(error) = entry.extract(&dest_path) {
                        return WalkPredicate::Stop(error);
                    }

                    if !entry.is_directory() {
                        link_map.insert(entry.entry_id(), dest_path);
                    }
                }
            }

            WalkPredicate::Continue
        });

        match walk_result {
            Err(crate::Error::NotDirectory) => Ok(()),
            Err(error) => Err(error),
            Ok(None) => Ok(()),
            Ok(Some(error)) => Err(error),
        }
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of paths of files with corrupt data or metadata.
    ///
    /// If you just need to verify the integrity of one object, [`Object::verify`] is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`Object::verify`]: crate::repo::Object::verify
    pub fn verify(&self) -> crate::Result<HashSet<RelativePathBuf>> {
        let corrupt_keys = self.repo.verify()?;
        Ok(self
            .repo
            .state()
            .tree
            .descendants(&*EMPTY_PATH)
            .unwrap()
            .filter(|(_, entry_handle)| {
                let entry_corrupt = corrupt_keys.contains(&entry_handle.entry);
                let file_corrupt = match &entry_handle.kind {
                    HandleType::File(object_id) => corrupt_keys.contains(object_id),
                    _ => false,
                };
                entry_corrupt || file_corrupt
            })
            .map(|(path, _)| path)
            .collect())
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.repo.clear_instance()
    }

    /// Change the password for this repository.
    ///
    /// See [`KeyRepo::change_password`] for details.
    ///
    /// [`KeyRepo::change_password`]: crate::repo::key::KeyRepo::change_password
    pub fn change_password(
        &mut self,
        new_password: &[u8],
        memory_limit: ResourceLimit,
        operations_limit: ResourceLimit,
    ) {
        self.repo
            .change_password(new_password, memory_limit, operations_limit);
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> InstanceId {
        self.repo.instance()
    }

    /// Compute statistics about the repository.
    ///
    /// See [`KeyRepo::stats`] for details.
    ///
    /// [`KeyRepo::stats`]: crate::repo::key::KeyRepo::stats
    pub fn stats(&self) -> RepoStats {
        self.repo.stats()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.repo.info()
    }
}

impl<S, M> Commit for FileRepo<S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    fn commit(&mut self) -> crate::Result<()> {
        self.repo.commit()
    }

    fn rollback(&mut self) -> crate::Result<()> {
        self.repo.rollback()
    }

    fn clean(&mut self) -> crate::Result<()> {
        self.repo.clean()
    }
}
impl<S, M> RestoreSavepoint for FileRepo<S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    type Restore = <StateRepo<RepoState> as RestoreSavepoint>::Restore;

    fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.repo.savepoint()
    }

    fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Self::Restore> {
        self.repo.start_restore(savepoint)
    }

    fn finish_restore(&mut self, restore: Self::Restore) -> bool {
        self.repo.finish_restore(restore)
    }
}

/// The default mount options which are always passed to libfuse.
const DEFAULT_FUSE_MOUNT_OPTS: &[&str] = &["-o", "default_permissions"];

#[cfg(all(any(unix, doc), feature = "fuse-mount"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "fuse-mount"))))]
impl FileRepo<UnixSpecial, UnixMetadata> {
    /// Mount the `FileRepo` as a FUSE file system.
    ///
    /// This accepts the path of the `root` entry in the repository which will be mounted in the
    /// file system at `mountpoint`. This also accepts an array of mount `options` to pass to
    /// libfuse.
    ///
    /// This method does not return until the file system is unmounted.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The given `root` path is empty.
    /// - `Error::NotFound`: There is no entry at `root`.
    /// - `Error::NotDirectory`: The given `root` entry is not a directory.
    /// - `Error::Io`: An I/O error occurred.
    pub fn mount(
        &mut self,
        mountpoint: impl AsRef<Path>,
        root: impl AsRef<RelativePath>,
        options: &[&str],
    ) -> crate::Result<()> {
        let adapter = FuseAdapter::new(self, root.as_ref())?;
        let all_opts = [DEFAULT_FUSE_MOUNT_OPTS, options]
            .concat()
            .into_iter()
            .map(|opt| opt.as_ref())
            .collect::<Vec<&OsStr>>();
        Ok(fuse::mount(adapter, &mountpoint, &all_opts)?)
    }
}

impl<S, M> Unlock for FileRepo<S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    fn unlock(&self) -> crate::Result<()> {
        self.repo.unlock()
    }

    fn is_locked(&self) -> crate::Result<bool> {
        self.repo.is_locked()
    }

    fn context(&self) -> crate::Result<Vec<u8>> {
        self.repo.context()
    }

    fn update_context(&self, context: &[u8]) -> crate::Result<()> {
        self.repo.update_context(context)
    }
}
