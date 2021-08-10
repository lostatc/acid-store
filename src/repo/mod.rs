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

//! High-level abstractions for data storage.
//!
//! This module provides abstractions for data storage called repositories. Each repository is
//! backed by a [`DataStore`], and provides features like encryption, compression, deduplication,
//! integrity checking, and atomic transactions.
//!
//! This module contains types which are common to most repositories. The most important of these
//! are [`Object`] and [`ReadOnlyObject`], which provide views of data in a repository and are used
//! to read data from them and write data to them.
//!
//! Each sub-module of this module contains a different repository type. If you're not sure which
//! one you should use, [`KeyRepo`] has the most general use-case.
//!
//! You can open or create a repository using [`OpenOptions`].
//!
//! # Deduplication
//! Data in a repository is transparently deduplicated using either fixed-size chunking (faster) or
//! contend-defined chunking (better deduplication). The chunk size and chunking method are
//! configured when you create a repository. See [`Chunking`] for details.
//!
//! # Locking
//! A repository cannot be open more than once simultaneously. Once a repository is opened, it is
//! locked exclusively until the repository is dropped. Repository locks are stored in the data
//! store and are respected by other processes and machines trying to open the repository.
//!
//! When a repository is dropped, it will attempt to release its lock on the data store. However,
//! releasing a lock can fail for a number of reasons, such as an I/O error or the thread panicking.
//! In this case, the repository will remain locked. You can use [`Unlock::unlock`] to manually
//! release a repository's lock on the data store while handling errors.
//!
//! Because releasing a repository lock can fail, it may be necessary to implement logic for
//! removing stale locks. When you open a repository with [`OpenOptions`], you can register a lock
//! handler that is invoked when an existing lock on the repository is detected and decides
//! whether to respect the existing lock or remove it. See [`OpenOptions::locking`] for details.
//!
//! **Removing an existing lock is potentially dangerous, as concurrent access to a repository can
//! cause data loss.**
//!
//! # Atomicity
//! Changes made to a repository are not persisted to the data store until those changes are
//! committed. Committing a repository is an atomic and consistent operation; changes cannot be
//! partially committed and interrupting a commit will never leave the repository in an inconsistent
//! state. If the repository is dropped or the thread panics, any uncommitted changes are rolled
//! back automatically. You can use [`Commit::commit`] to commit changes to a repository.
//!
//! When data in a repository is deleted, the space is not reclaimed in the backing data store until
//! those changes are committed and the repository is cleaned. Cleaning a repository can be an
//! expensive operation, so these are kept as separate steps so that it is possible to commit
//! changes without cleaning the repository. You can use [`Commit::clean`] to clean a repository.
//!
//! Repositories support creating savepoints and then later restoring to those savepoints to
//! atomically undo or redo changes to a repository without rolling back all changes made since the
//! last commit. See [`RestoreSavepoint`] for more information.
//!
//! # Encryption
//! If encryption is enabled, the Argon2id key derivation function is used to derive a key from a
//! user-supplied password. This key is used to encrypt the repository's randomly generated master
//! key, which is used to encrypt all data in the repository. This setup means that the repository's
//! password can be changed without re-encrypting any data.
//!
//! Data in a data store is identified by random UUIDs and not hashes, so data hashes are not
//! leaked. By default, the repository does not attempt to hide the size of chunks produced by the
//! chunking algorithm, which is a form of metadata leakage which may be undesirable in some cases.
//! To fix this, you can configure the repository to pack data into fixed-size blocks before writing
//! it to the data store at the cost of performance. See [`Packing`] for details.
//!
//! The information in [`RepoInfo`] is never encrypted, and can be read without decrypting the
//! repository using [`peek_info`].
//!
//! # Instances
//! A repository can consist of multiple instances, each identified by an [`InstanceId`]. Each
//! repository instance has completely separate contents, meaning that data in one instance won't
//! appear in others.
//!
//! You can specify the ID of the instance you want to access when you open or create a repository
//! using [`OpenOptions`]. You can also switch from one instance to another using
//! [`SwitchInstance::switch_instance`].
//!
//! Different repository instances share the same underlying storage, meaning that they share
//! the same configuration, they are encrypted using the same password, and data is deduplicated
//! between them. This also means that only one instance of a repository can be open at a time.
//!
//! Instances of the same repository can be different repository types. This feature allows for
//! having multiple repositories of different types which are backed by the same [`DataStore`]. For
//! example, you could have a data store which contains both a [`FileRepo`] and a [`VersionRepo`] by
//! giving them different instance IDs, and data will still be deduplicated between them.
//!
//! This feature can also be used to manage memory usage. The amount of memory used by a repository
//! while it's open is typically proportional to the number of objects in the repository. If you
//! split your data between multiple repository instances, only the currently open instance will
//! need to store data in memory.
//!
//! Switching repository instances does not commit or roll back changes. Committing changes to a
//! repository commits changes for all instances of that repository; it is not possible to commit
//! changes to only a single instance. The same goes for rolling back changes.
//!
//! [`DataStore`]: crate::store::DataStore
//! [`Object`]: crate::repo::Object
//! [`ReadOnlyObject`]: crate::repo::ReadOnlyObject
//! [`KeyRepo`]: crate::repo::key::KeyRepo
//! [`OpenOptions`]: crate::repo::OpenOptions
//! [`Chunking`]: crate::repo::Chunking
//! [`Unlock::unlock`]: crate::repo::Unlock::unlock
//! [`OpenOptions::locking`]: crate::repo::OpenOptions::locking
//! [`Commit::commit`]: crate::repo::Commit::commit
//! [`Commit::clean`]: crate::repo::Commit::clean
//! [`RestoreSavepoint`]: crate::repo::RestoreSavepoint
//! [`Packing`]: crate::repo::Packing
//! [`RepoInfo`]: crate::repo::RepoInfo
//! [`peek_info`]: crate::repo::peek_info
//! [`InstanceId`]: crate::repo::InstanceId
//! [`SwitchInstance::switch_instance`]: crate::repo::SwitchInstance::switch_instance
//! [`FileRepo`]: crate::repo::file::FileRepo
//! [`VersionRepo`]: crate::repo::version::VersionRepo

pub use self::common::{
    peek_info, Chunking, Commit, Compression, ContentId, Encryption, InstanceId, Object, ObjectId,
    ObjectStats, OpenMode, OpenOptions, OpenRepo, Packing, ReadOnlyObject, RepoConfig, RepoId,
    RepoInfo, RepoStats, ResourceLimit, Restore, RestoreSavepoint, Savepoint, SwitchInstance,
    Unlock, VersionId, DEFAULT_INSTANCE,
};

/// An object store which maps keys to seekable binary blobs.
///
/// This module contains the [`KeyRepo`] repository type.
///
/// A [`KeyRepo`] maps keys to seekable binary blobs called objects and stores them persistently in
/// a [`DataStore`]. A key is any type which implements [`Key`].
///
/// Like other repositories, changes made to the repository are not persisted to the data store
/// until [`Commit::commit`] is called. For details about deduplication, compression, encryption,
/// and locking, see the module-level documentation for [`crate::repo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
/// [`DataStore`]: crate::store::DataStore
/// [`Key`]: crate::repo::key::Key
/// [`Commit::commit`]: crate::repo::Commit::commit
pub mod key {
    pub use super::common::{Key, KeyRepo, Keys};
}

mod common;

#[cfg(feature = "repo-content")]
#[cfg_attr(docsrs, doc(cfg(feature = "repo-content")))]
pub mod content;

#[cfg(feature = "repo-file")]
#[cfg_attr(docsrs, doc(cfg(feature = "repo-file")))]
pub mod file;

pub mod state;

#[cfg(feature = "repo-value")]
#[cfg_attr(docsrs, doc(cfg(feature = "repo-value")))]
pub mod value;

#[cfg(feature = "repo-version")]
#[cfg_attr(docsrs, doc(cfg(feature = "repo-version")))]
pub mod version;
