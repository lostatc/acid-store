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

use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::{create_dir, create_dir_all, metadata, File, OpenOptions};
use std::io::{self, copy, Write};
use std::marker::PhantomData;
use std::path::Path;

use hex_literal::hex;
use once_cell::sync::Lazy;
use relative_path::{RelativePath, RelativePathBuf};
use uuid::Uuid;
use walkdir::WalkDir;

use crate::repo::id_table::UniqueId;
use crate::repo::{
    key::KeyRepo, state_repo::StateRepo, Object, OpenRepo, ReadOnlyObject, RepoInfo, Savepoint,
};

use super::entry::{Entry, EntryHandle, EntryType, FileType};
use super::metadata::{FileMetadata, NoMetadata};
use super::path_tree::PathTree;
use super::special::{NoSpecialType, SpecialType};
use super::state::{FileRepoKey, FileRepoState, Restore, STATE_KEYS};

/// The parent of a relative path with no parent.
static EMPTY_PARENT: Lazy<RelativePathBuf> = Lazy::new(|| RelativePath::new("").to_owned());

/// A virtual file system.
///
/// See [`crate::repo::file`] for more information.
#[derive(Debug)]
pub struct FileRepo<S = NoSpecialType, M = NoMetadata>(
    StateRepo<FileRepoKey, FileRepoState>,
    PhantomData<(S, M)>,
)
where
    S: SpecialType,
    M: FileMetadata;

impl<S, M> OpenRepo for FileRepo<S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    type Key = FileRepoKey;

    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("ea34b5c4 be47 11eb b4c3 0fc0fea79bb3"));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state_repo = StateRepo {
            repo,
            state: FileRepoState::default(),
            keys: STATE_KEYS,
        };
        state_repo.state = state_repo.read_state()?;
        Ok(FileRepo(state_repo, PhantomData))
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state_repo = StateRepo {
            repo,
            state: FileRepoState::default(),
            keys: STATE_KEYS,
        };
        state_repo.write_state()?;
        Ok(FileRepo(state_repo, PhantomData))
    }

    fn into_repo(mut self) -> crate::Result<KeyRepo<Self::Key>> {
        self.0.write_state()?;
        Ok(self.0.repo)
    }
}

impl<S, M> FileRepo<S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    /// Remove the object with the given `object_id` from the backing repository.
    fn remove_id(&mut self, object_id: UniqueId) -> bool {
        if !self.0.state.id_table.recycle(object_id) {
            return false;
        }
        if !self.0.repo.remove(&FileRepoKey::Object(object_id)) {
            panic!("Object ID was in use but not found in backing repository.");
        }
        true
    }

    /// Return whether there is an entry at `path`.
    pub fn exists(&self, path: impl AsRef<RelativePath>) -> bool {
        self.0.state.path_table.contains(path.as_ref())
    }

    /// Return `true` if the given `path` has a parent directory in the repository.
    fn has_parent(&self, path: &RelativePath) -> bool {
        match path.parent() {
            Some(parent) if parent != *EMPTY_PARENT => match self.0.state.path_table.get(parent) {
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
    /// # use acid_store::repo::file::{FileRepo, Entry, RelativePath, UnixSpecialType};
    /// # use acid_store::store::{MemoryStore, MemoryConfig};
    /// #
    /// # let mut repo: FileRepo<UnixSpecialType> = OpenOptions::new()
    /// #    .mode(OpenMode::CreateNew)
    /// #    .open(&MemoryConfig::new())
    /// #    .unwrap();
    /// #
    /// let entry_path = RelativePath::new("link");
    /// let symbolic_link = UnixSpecialType::SymbolicLink {
    ///     target: Path::new("target").to_owned()
    /// };
    /// repo.create(entry_path, &Entry::special(symbolic_link)).unwrap();
    /// # }
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
        entry: &Entry<S, M>,
    ) -> crate::Result<()> {
        if self.exists(&path) {
            return Err(crate::Error::AlreadyExists);
        }

        if !self.has_parent(path.as_ref()) {
            return Err(crate::Error::InvalidPath);
        }

        // Write the entry for the file.
        let entry_id = self.0.state.id_table.next();
        let mut object = self.0.repo.insert(FileRepoKey::Object(entry_id));
        object.serialize(entry)?;
        drop(object);

        let entry_type = match entry.file_type {
            FileType::File => {
                let file_id = self.0.state.id_table.next();
                self.0.repo.insert(FileRepoKey::Object(file_id));
                EntryType::File(file_id)
            }
            FileType::Directory => EntryType::Directory,
            FileType::Special(_) => EntryType::Special,
        };

        let handle = EntryHandle {
            entry: entry_id,
            entry_type,
        };

        self.0.state.path_table.insert(path.as_ref(), handle);

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
        entry: &Entry<S, M>,
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
    /// The space used by the given entry isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotEmpty`: The entry is a directory which is not empty.
    ///
    /// [`clean`]: crate::repo::file::FileRepo::clean
    pub fn remove(&mut self, path: impl AsRef<RelativePath>) -> crate::Result<()> {
        match self.0.state.path_table.list(&path) {
            Some(mut children) => {
                if children.next().is_some() {
                    return Err(crate::Error::NotEmpty);
                }
            }
            None => return Err(crate::Error::NotFound),
        }

        let entry_handle = self.0.state.path_table.remove(path.as_ref()).unwrap();
        if let EntryType::File(object_id) = entry_handle.entry_type {
            self.remove_id(object_id);
        }
        self.remove_id(entry_handle.entry);

        Ok(())
    }

    /// Remove the entry with the given `path` and its descendants from the repository.
    ///
    /// The space used by the given entry isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    ///
    /// [`clean`]: crate::repo::file::FileRepo::clean
    pub fn remove_tree(&mut self, path: impl AsRef<RelativePath>) -> crate::Result<()> {
        let handles = self
            .0
            .state
            .path_table
            .drain(path.as_ref())
            .ok_or(crate::Error::NotFound)?
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();

        for handle in handles {
            if let EntryType::File(object_id) = &handle.entry_type {
                self.remove_id(*object_id);
            }
            self.remove_id(handle.entry);
        }

        Ok(())
    }

    /// Return the entry at `path`.
    ///
    /// # Examples
    /// Check if an entry is a regular file.
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
    /// assert!(repo.entry(entry_path).unwrap().is_file())
    /// ```
    /// # Errors
    /// - `Error::NotFound`: There is no entry at `path`.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn entry(&self, path: impl AsRef<RelativePath>) -> crate::Result<Entry<S, M>> {
        let entry_handle = &self
            .0
            .state
            .path_table
            .get(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let mut object = self
            .0
            .repo
            .object(&FileRepoKey::Object(entry_handle.entry))
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
            .0
            .state
            .path_table
            .get_mut(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        let mut object = self
            .0
            .repo
            .object_mut(&FileRepoKey::Object(entry_handle.entry))
            .unwrap();
        let mut entry: Entry<S, M> = object.deserialize()?;
        entry.metadata = metadata;
        object.serialize(&entry)
    }

    /// Return a `ReadOnlyObject` for reading the contents of the file at `path`.
    ///
    /// The returned object provides read-only access to the file. To get read-write access, use
    /// [`open_mut`].
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotFile`: The entry does not represent a regular file.
    ///
    /// [`open_mut`]: crate::repo::file::FileRepo::open_mut
    pub fn open(&self, path: impl AsRef<RelativePath>) -> crate::Result<ReadOnlyObject> {
        let entry_handle = self
            .0
            .state
            .path_table
            .get(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        if let EntryType::File(object_id) = &entry_handle.entry_type {
            Ok(self
                .0
                .repo
                .object(&FileRepoKey::Object(*object_id))
                .unwrap())
        } else {
            Err(crate::Error::NotFile)
        }
    }

    /// Return an `Object` for reading and writing the contents of the file at `path`.
    ///
    /// The returned object provides read-write access to the file. To get read-only access, use
    /// [`open`].
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no entry with the given `path`.
    /// - `Error::NotFile`: The entry does not represent a regular file.
    ///
    /// [`open`]: crate::repo::file::FileRepo::open
    pub fn open_mut(&mut self, path: impl AsRef<RelativePath>) -> crate::Result<Object> {
        let entry_handle = self
            .0
            .state
            .path_table
            .get_mut(path.as_ref())
            .ok_or(crate::Error::NotFound)?;
        match entry_handle.entry_type {
            EntryType::File(object_id) => Ok(self
                .0
                .repo
                .object_mut(&FileRepoKey::Object(object_id))
                .unwrap()),
            _ => Err(crate::Error::NotFile),
        }
    }

    /// Create and return a copy of the given `EntryHandle`.
    fn copy_entry_handle(&mut self, handle: &EntryHandle) -> EntryHandle {
        let new_entry_id = self.0.state.id_table.next();
        self.0.repo.copy(
            &FileRepoKey::Object(handle.entry),
            FileRepoKey::Object(new_entry_id),
        );
        EntryHandle {
            entry: new_entry_id,
            entry_type: match &handle.entry_type {
                EntryType::File(file_id) => {
                    let new_file_id = self.0.state.id_table.next();
                    self.0.repo.copy(
                        &FileRepoKey::Object(*file_id),
                        FileRepoKey::Object(new_file_id),
                    );
                    EntryType::File(new_file_id)
                }
                EntryType::Directory => EntryType::Directory,
                EntryType::Special => EntryType::Special,
            },
        }
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
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    ///
    /// [`archive`]: crate::repo::file::FileRepo::archive
    /// [`extract`]: crate::repo::file::FileRepo::extract
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
            .0
            .state
            .path_table
            .get(source.as_ref())
            .ok_or(crate::Error::NotFound)?
            .clone();

        let new_handle = self.copy_entry_handle(&entry_handle);
        self.0.state.path_table.insert(dest.as_ref(), new_handle);

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
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::NotFound`: There is no entry at `source`.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    ///
    /// [`archive_tree`]: crate::repo::file::FileRepo::archive
    /// [`extract_tree`]: crate::repo::file::FileRepo::extract
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

        // Because we can't walk the path tree and insert into it at the same time, we need to
        // construct a tree of the destination paths before inserting them back into the path table.
        // Using this prefix tree type should consume less memory than collecting the paths into a
        // vector.
        let mut dest_tree = PathTree::new();

        // Copy the root path into the destination tree.
        let source_root_handle = self
            .0
            .state
            .path_table
            .get(source.as_ref())
            .ok_or(crate::Error::NotFound)?
            .clone();
        let dest_root_handle = self.copy_entry_handle(&source_root_handle);
        self.0.state.path_table.insert(&dest, dest_root_handle);
        dest_tree.insert(&dest, source_root_handle);

        // Get the destination paths for each path in the path table and insert them into the
        // destination tree.
        for (path, source_handle) in self.0.state.path_table.walk(source.as_ref()).unwrap() {
            let relative_path = path.strip_prefix(&source).unwrap();
            let dest_path = dest.as_ref().join(relative_path);
            dest_tree.insert(dest_path, source_handle.clone());
        }

        // Move the rest of the paths from the destination tree into the path table.
        for (dest_path, source_handle) in dest_tree.drain(&dest).unwrap() {
            let dest_handle = self.copy_entry_handle(&source_handle);
            self.0.state.path_table.insert(dest_path, dest_handle);
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
            .0
            .state
            .path_table
            .get(parent.as_ref())
            .ok_or(crate::Error::NotFound)?;
        if !matches!(entry_handle.entry_type, EntryType::Directory) {
            return Err(crate::Error::NotDirectory);
        }

        Ok(self
            .0
            .state
            .path_table
            .list(parent)
            .unwrap()
            .map(|(path, _)| path))
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
            .0
            .state
            .path_table
            .get(parent.as_ref())
            .ok_or(crate::Error::NotFound)?;
        if !matches!(entry_handle.entry_type, EntryType::Directory) {
            return Err(crate::Error::NotDirectory);
        }

        Ok(self
            .0
            .state
            .path_table
            .walk(parent)
            .unwrap()
            .map(|(path, _)| path))
    }

    /// Copy a file from the file system into the repository.
    ///
    /// If `source` is a directory, its descendants are not copied.
    ///
    /// The `source` file's metadata will be copied to the `dest` entry according to the selected
    /// [`FileMetadata`] implementation.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
    /// - `Error::AlreadyExists`: There is already an entry at `dest`.
    /// - `Error::FileType`: The file at `source` is not a regular file, directory, or supported
    /// special file.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`FileMetadata`]: crate::repo::file::FileMetadata
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
            FileType::Special(S::from_file(source.as_ref())?.ok_or(crate::Error::FileType)?)
        };

        let entry = Entry {
            file_type,
            metadata: Some(M::from_file(source.as_ref())?),
        };

        self.create(&dest, &entry)?;

        // Write the contents of the file entry if it's a file.
        let entry_handle = self.0.state.path_table.get_mut(dest.as_ref()).unwrap();
        if let EntryType::File(object_id) = entry_handle.entry_type {
            let mut object = self
                .0
                .repo
                .object_mut(&FileRepoKey::Object(object_id))
                .unwrap();
            let mut file = File::open(&source)?;
            copy(&mut file, &mut object)?;
            object.flush()?;
        }

        Ok(())
    }

    /// Copy a directory tree from the file system into the repository.
    ///
    /// If `source` is a directory, this also copies its descendants. If `source` is not a
    /// directory, this is the same as calling [`archive`]. If one of the files in the tree is not a
    /// regular file, directory, or supported special file, it is skipped.
    ///
    /// The `source` file's metadata will be copied to the `dest` entry according to the selected
    /// [`FileMetadata`] implementation.
    ///
    /// # Errors
    /// - `Error::InvalidPath`: The parent of `dest` does not exist or is not a directory.
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
    /// # Errors
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`FileMetadata`]: crate::repo::file::FileMetadata
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
    /// directory, this is the same as calling [`extract`].
    ///
    /// The `source` entry's metadata will be copied to the `dest` file according to the selected
    /// [`FileMetadata`] implementation.
    ///
    /// # Errors
    /// - `Error::NotFound`: The `source` entry does not exist.
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`extract`]: crate::repo::file::FileRepo::extract
    /// [`FileMetadata`]: crate::repo::file::FileMetadata
    pub fn extract_tree(
        &self,
        source: impl AsRef<RelativePath>,
        dest: impl AsRef<Path>,
    ) -> crate::Result<()> {
        let relative_descendants = self
            .0
            .state
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
    /// See [`KeyRepo::commit`] for details.
    ///
    /// [`KeyRepo::commit`]: crate::repo::key::KeyRepo::commit
    pub fn commit(&mut self) -> crate::Result<()> {
        self.0.commit()
    }

    /// Roll back all changes made since the last commit.
    ///
    /// See [`KeyRepo::rollback`] for details.
    ///
    /// [`KeyRepo::rollback`]: crate::repo::key::KeyRepo::rollback
    pub fn rollback(&mut self) -> crate::Result<()> {
        self.0.rollback()
    }

    /// Create a new `Savepoint` representing the current state of the repository.
    ///
    /// See [`KeyRepo::savepoint`] for details.
    ///
    /// [`KeyRepo::savepoint`]: crate::repo::key::KeyRepo::savepoint
    pub fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.0.savepoint()
    }

    /// Start the process of restoring the repository to the given `savepoint`.
    ///
    /// See [`KeyRepo::start_restore`] for details.
    ///
    /// [`KeyRepo::start_restore`]: crate::repo::key::KeyRepo::start_restore
    pub fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Restore> {
        Ok(Restore(self.0.start_restore(savepoint)?))
    }

    /// Finish the process of restoring the repository to a [`Savepoint`].
    ///
    /// See [`KeyRepo::finish_restore`] for details.
    ///
    /// [`Savepoint`]: crate::repo::Savepoint
    /// [`KeyRepo::finish_restore`]: crate::repo::key::KeyRepo::finish_restore
    pub fn finish_restore(&mut self, restore: Restore) -> bool {
        self.0.finish_restore(restore.0)
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// See [`KeyRepo::clean`] for details.
    ///
    /// [`KeyRepo::clean`]: crate::repo::key::KeyRepo::clean
    pub fn clean(&mut self) -> crate::Result<()> {
        self.0.repo.clean()
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.0.clear_instance()
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
        let corrupt_keys = self.0.repo.verify()?;
        Ok(self
            .0
            .state
            .path_table
            .walk(RelativePathBuf::new())
            .unwrap()
            .filter(|(_, entry_handle)| {
                let entry_corrupt = corrupt_keys.contains(&FileRepoKey::Object(entry_handle.entry));
                let file_corrupt = match &entry_handle.entry_type {
                    EntryType::File(object_id) => {
                        corrupt_keys.contains(&FileRepoKey::Object(*object_id))
                    }
                    _ => false,
                };
                entry_corrupt || file_corrupt
            })
            .map(|(path, _)| path)
            .collect::<HashSet<_>>())
    }

    /// Change the password for this repository.
    ///
    /// See [`KeyRepo::change_password`] for details.
    ///
    /// [`KeyRepo::change_password`]: crate::repo::key::KeyRepo::change_password
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.0.repo.change_password(new_password);
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> Uuid {
        self.0.repo.instance()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.0.repo.info()
    }
}
