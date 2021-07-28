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

use std::iter::{ExactSizeIterator, FusedIterator};
use std::path::Path;

use relative_path::{RelativePath, RelativePathBuf};

use super::entry::{Entry, EntryHandle, HandleType};
use super::metadata::FileMetadata;
use super::path_tree;
use super::repository::FileRepo;
use super::special::SpecialType;
use crate::repo::Object;

/// An iterator over the children of an entry in a [`FileRepo`].
///
/// This value is created by [`FileRepo::children`].
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`FileRepo::children`]: crate::repo::file::FileRepo::children
#[derive(Debug, Clone)]
pub struct Children<'a>(pub(super) path_tree::Children<'a, EntryHandle>);

impl<'a> Iterator for Children<'a> {
    type Item = RelativePathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(path, _)| path)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a> FusedIterator for Children<'a> {}

impl<'a> ExactSizeIterator for Children<'a> {}

/// An iterator over the descendants of an entry in a [`FileRepo`].
///
/// This value is created by [`FileRepo::descendants`].
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`FileRepo::descendants`]: crate::repo::file::FileRepo::descendants
#[derive(Debug)]
pub struct Descendants<'a>(pub(super) path_tree::Descendants<'a, EntryHandle>);

impl<'a> Iterator for Descendants<'a> {
    type Item = RelativePathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(path, _)| path)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

/// A value that controls which entries are visited by [`FileRepo::walk`].
///
/// [`FileRepo::walk`]: crate::repo::file::FileRepo::walk
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalkPredicate<R> {
    /// Continue visiting entries in the tree.
    Continue,

    /// Skip the remaining siblings of this entry.
    SkipSiblings,

    /// Do not visit the descendants of this entry.
    SkipDescendants,

    /// Stop visiting entries and return early with the given value.
    Stop(R),
}

/// An entry when walking through a tree of entries in a [`FileRepo`].
///
/// This value represent an entry when walking through tree of entries using [`FileRepo::walk`].
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`FileRepo::walk`]: crate::repo::file::FileRepo::walk
pub struct WalkEntry<'a, S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    pub(super) path: RelativePathBuf,
    pub(super) handle: EntryHandle,
    pub(super) depth: usize,
    pub(super) repo: &'a FileRepo<S, M>,
}

impl<'a, S, M> AsRef<RelativePath> for WalkEntry<'a, S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    fn as_ref(&self) -> &RelativePath {
        self.path.as_relative_path()
    }
}

impl<'a, S, M> WalkEntry<'a, S, M>
where
    S: SpecialType,
    M: FileMetadata,
{
    /// Return the path of this entry.
    pub fn path(&self) -> &RelativePath {
        self.path.as_relative_path()
    }

    /// Consume this entry, returning its path.
    pub fn into_path(self) -> RelativePathBuf {
        self.path
    }

    /// Return whether this entry is a regular file.
    pub fn is_file(&self) -> bool {
        matches!(self.handle.kind, HandleType::File(_))
    }

    /// Return whether this entry is a directory.
    pub fn is_directory(&self) -> bool {
        matches!(self.handle.kind, HandleType::Directory)
    }

    /// Return whether this entry is a special file.
    pub fn is_special(&self) -> bool {
        matches!(self.handle.kind, HandleType::Special)
    }

    /// Return the depth of this entry relative to the starting path.
    ///
    /// The immediate children of the starting path have a depth of `1`, their children have a
    /// depth of `2`, and so on.
    pub fn depth(&self) -> usize {
        self.depth
    }

    /// Return the `Entry` value for this entry.
    ///
    /// # Errors
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn entry(&self) -> crate::Result<Entry<S, M>> {
        self.repo.entry(&self.path)
    }

    /// Return an `Object` for reading and writing the contents of this entry.
    ///
    /// This returns `None` if this entry is not a regular file.
    pub fn open(&self) -> Option<Object> {
        match self.repo.open(&self.path) {
            Ok(object) => Some(object),
            Err(crate::Error::NotFile) => None,
            Err(error) => panic!("{:?}", error),
        }
    }

    /// Copy this entry from the repository into the file system.
    ///
    /// If this entry is a directory, its descendants are not copied.
    ///
    /// This entry’s metadata will be copied to the `dest` file according to the selected
    /// [`FileMetadata`] implementation.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: The `dest` file already exists.
    /// - `Error::Deserialize`: The file metadata could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`FileMetadata`]: crate::repo::file::FileMetadata
    pub fn extract(&self, dest: impl AsRef<Path>) -> crate::Result<()> {
        match self.repo.extract(&self.path, dest) {
            Err(error @ crate::Error::InvalidPath | error @ crate::Error::NotFound) => {
                panic!("{:?}", error)
            }
            result => result,
        }
    }
}
