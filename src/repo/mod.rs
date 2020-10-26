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

//! High-level abstractions for data storage.
//!
//! This module provides abstractions for data storage called repositories. Each repository is
//! backed by a `DataStore`, and provides features like encryption, compression, deduplication,
//! integrity checking, locking, and atomic transactions.
//!
//! This module contains types which are common to most repositories. The most important of these
//! are `Object` and `ReadOnlyObject`, which provide views of data in a repository and are used to
//! read data from them and write data to them.
//!
//! Each sub-module of this module contains a different repository type. If you're not sure which
//! one you should use, `KeyRepo` has the most general use-case.
//!
//! You can open or create a repository using `OpenOptions`.
//!
//! # Deduplication
//! Data in a repository is transparently deduplicated using either fixed-size chunking (faster) or
//! contend-defined chunking (better deduplication). The chunk size and chunking method are
//! configured when you create a repository. See `Chunking` for details.
//!
//! # Locking
//! A repository cannot be open more than once simultaneously. Once it is opened, it is locked from
//! further open attempts until the repository is dropped. This lock prevents the repository from
//! being opened from other threads and processes on the same machine, but not from other machines.
//!
//! # Atomicity
//! Changes made to a repository are not persisted to the data store until `commit` is called. If
//! the repository is dropped or the thread panics, any uncommitted changes are rolled back
//! automatically.
//!
//! # Encryption
//! If encryption is enabled, the Argon2id key derivation function is used to derive a key from a
//! user-supplied password. This key is used to encrypt the repository's randomly generated master
//! key, which is used to encrypt all data in the repository. This setup means that the repository's
//! password can be changed without re-encrypting any data.
//!
//! The master key is generated using the operating system's secure random number generator. Both
//! the master key and the derived key are zeroed in memory once they go out of scope.
//!
//! Data in a data store is identified by UUIDs and not hashes, so data hashes are not leaked. The
//! repository does not attempt to hide the size of chunks produced by the chunking algorithm, but
//! information about which chunks belong to which objects is encrypted.
//!
//! The information in `RepoInfo` is never encrypted, and can be read without opening the
//! repository.
//!
//! # Instances
//! A repository can consist of multiple instances, each identified by a UUID. Each repository
//! instance has completely separate contents, meaning that data in one instance won't appear in
//! others.
//!
//! You can specify the ID of the instance you want to access when you open or create a repository
//! using `OpenOptions`. You can also switch from one instance to another using
//! `ConvertRepo::switch_instance`.
//!
//! Different repository instances share the same underlying storage, meaning that they share
//! the same configuration, they are encrypted using the same password, and data is deduplicated
//! between them. This also means that only one instance of a repository can be open at a time.
//!
//! This feature allows for using multiple repository types within the same `DataStore`. For
//! example, you could have a data store which contains both a `FileRepo` and a
//! `VersionRepo` by giving them different instance IDs.
//!
//! This feature can also be used to manage memory usage. The amount of memory used by a repository
//! while it's open is typically proportional to the number of objects in the repository. If you
//! split your data between multiple repository instances, only the currently open instance will
//! need to store data in memory.

pub use self::common::{
    Chunking, Compression, ContentId, ConvertRepo, Encryption, LockStrategy, Object, OpenOptions,
    ReadOnlyObject, RepoConfig, RepoInfo, ResourceLimit,
};

/// A low-level repository type which provides more direct access to the underlying storage.
///
/// This module contains the `ObjectRepo` repository type.
///
/// This repository type is mostly intended to be used to create other, higher-level repository
/// types. All the other repository types in `acid_store::repo` are implemented on top of it. Its
/// API is more complicated than the other repository types, but it provides more control over how
/// data is stored and how memory is managed.
///
/// Repository types which are implemented on top of `ObjectRepo` can implement `ConvertRepo`, which
/// allows them to be opened or created using `OpenOptions` and also allows for easily switching
/// between repository instances of different types.
///
/// Like other repositories, changes made to the repository are not persisted to the data store
/// until `ObjectRepo::commit` is called. For details about deduplication, compression, encryption,
/// and locking, see the module-level documentation for `acid_store::repo`.
///
/// # Managed and unmanaged objects
/// An `ObjectRepo` has two modes for storing data, *managed* objects and *unmanaged* objects.
///
/// Unmanaged objects are accessed via an `ObjectHandle`. Object handles are not stored in the
/// repository, and it's the user's responsibility to keep track of them. Without an object handle,
/// you cannot access or remove the data associated with it.
///
/// Managed objects are also accessed via object handles, but these object handles are stored in the
/// repository and the user doesn't have to worry about keeping track of them. Each managed object
/// is associated with a UUID which can be used to access or remove the data.
///
/// If your repository has many objects, you may not want to store all the object handles in memory,
/// since they take up a non-trivial amount of space. Object handles are always stored in memory for
/// managed objects, but not necessarily for unmanaged objects. `ObjectHandle` is serializable, so
/// it can be stored in other managed or unmanaged objects.
///
/// However, if `ObjectRepo` only had unmanaged objects, and all the object handles were
/// stored in other unmanaged objects, you would have a chicken-and-egg problem and wouldn't be able
/// to access any data! This is where managed objects are useful. They can be used to store
/// object handles (and other data) with a predictable UUID, potentially set at compile time.
pub mod object {
    pub use super::common::{IntegrityReport, ObjectHandle, ObjectRepo};
}

mod common;
pub mod content;
pub mod file;
pub mod key;
pub mod value;
pub mod version;
