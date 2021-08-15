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

//! A virtual file system.
//!
//! This module contains the [`FileRepo`] repository type.
//!
//! This is a repository type which functions as a virtual file system. It supports file metadata,
//! special file types, and importing and exporting files from and to the local file system.
//!
//! A [`FileRepo`] is composed of [`Entry`] values which represent either a regular file, a
//! directory, or a special file. Files in the file system can be copied into the repository using
//! [`FileRepo::archive`] and [`FileRepo::archive_tree`], and entries in the repository can be
//! copied to the file system using [`FileRepo::extract`] and [`FileRepo::extract_tree`]. It is also
//! possible to manually add, remove, query, and modify entries.
//!
//! This repository is designed so that files archived on one platform can be extracted on another
//! platform. Because many aspects of file systems—such as file paths, file metadata, and special
//! file types—are heavily platform-dependent, the behavior of [`FileRepo`] can be customized
//! through the [`FileMetadata`] and [`SpecialType`] traits.
//!
//! A [`FileRepo`] can be mounted as a FUSE file system using [`FileRepo::mount`].
//!
//! Like other repositories, changes made to the repository are not persisted to the data store
//! until [`Commit::commit`] is called. For details about deduplication, compression, encryption,
//! and locking, see the module-level documentation for [`crate::repo`].
//!
//! # Paths
//!
//! While files in the file system are located using a `Path`, entries in the repository are located
//! using a [`RelativePath`], which is a platform-independent path representation. While some
//! platforms support arbitrary bytes sequences in file paths, a [`RelativePath`] must consist of
//! valid UTF-8.
//!
//! Because file system roots are platform-dependent, [`FileRepo`] does not have a root entry.
//! Instead, entry paths are relative paths relative to the root of the repository. A top-level
//! directory `foo` containing a file `bar` is represented as `foo/bar`.
//!
//! # Metadata
//!
//! A [`FileRepo`] accepts a [`FileMetadata`] type parameter which determines how it handles file
//! metadata. The default value is [`NoMetadata`], which means that it does not store any file
//! metadata. Other implementations are provided through the `file-metadata` cargo feature. If you
//! attempt to read an entry using a different [`FileMetadata`] implementation than it was stored
//! with, it will fail to deserialize and return an error.
//!
//! # Special Files
//!
//! A [`FileRepo`] accepts a [`SpecialType`] type parameter which determines how it handles
//! special file types. The default value is [`NoSpecial`], which means that it does not attempt
//! to handle file types beyond regular files and directories. Other implementations are provided
//! through the `file-metadata` cargo feature. If you attempt to read an entry using a different
//! [`SpecialType`] implementation than it was stored with, it will fail to deserialize and return
//! an error.
//!
//! [`FileRepo`]: crate::repo::file::FileRepo
//! [`Entry`]: crate::repo::file::Entry
//! [`FileRepo::archive`]: crate::repo::file::FileRepo::archive
//! [`FileRepo::archive_tree`]: crate::repo::file::FileRepo::archive_tree
//! [`FileRepo::extract`]: crate::repo::file::FileRepo::extract
//! [`FileRepo::extract_tree`]: crate::repo::file::FileRepo::extract_tree
//! [`RelativePath`]: crate::repo::file::RelativePath
//! [`FileMetadata`]: crate::repo::file::FileMetadata
//! [`SpecialType`]: crate::repo::file::SpecialType
//! [`FileRepo::mount`]: crate::repo::file::FileRepo::mount
//! [`Commit::commit`]: crate::repo::Commit::commit
//! [`NoMetadata`]: crate::repo::file::NoMetadata
//! [`NoSpecial`]: crate::repo::file::NoSpecial

pub use relative_path;

pub use relative_path::{RelativePath, RelativePathBuf};

#[cfg(all(unix, feature = "file-metadata"))]
pub use {
    self::metadata::{Acl, AclMode, AclQualifier, AclType, FileMode, UnixMetadata},
    self::special::UnixSpecial,
};

pub use self::entry::{Entry, EntryId, EntryType};
pub use self::iter::{Children, Descendants, WalkEntry, WalkPredicate};
#[cfg(feature = "file-metadata")]
pub use self::metadata::CommonMetadata;
pub use self::metadata::{FileMetadata, NoMetadata};
pub use self::repository::FileRepo;
pub use self::special::{NoSpecial, SpecialType};

mod entry;
mod file;
mod fuse;
mod iter;
mod metadata;
mod path_tree;
mod repository;
mod special;
