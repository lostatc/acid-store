/*
 * Copyright 2019 Wren Powell
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

//! `disk-archive` is a library for creating high-performance file formats.
//!
//! Many application file formats are built on ZIP archives or SQLite databases. ZIP archives are
//! difficult to update in-place, requiring the entire archive to be unpacked and repacked, which
//! can be expensive for large archives. SQLite databases aren't well-suited for storing large
//! binary blobs, which can be problematic for applications where this is a requirement.
//!
//! This crate provides a custom file format that is designed for storing large binary blobs. It's
//! meant to be used as an alternative to ZIP archives or SQLite databases for creating
//! high-performance file formats. The archive format supports atomic transactions and in-place
//! updates like SQLite while being more suited for storing large blobs.
//!
//! Archives created with this crate support the following features:
//! - Content-defined block-level deduplication
//! - Transparent compression
//! - Transparent encryption
//! - Integrity checking of data
//! - Atomicity, consistency, isolation, and durability (ACID)
//!
//! For applications which only need to store small (<100KiB) blobs, a SQLite database may be a
//! better choice. For applications which require an open/transparent format, consider using a ZIP
//! archive.
//!
//! This crate provides two abstractions over the archive format:
//! - `FileArchive` is a file archive like ZIP or TAR which supports modification times, POSIX file
//! permissions, extended attributes, and symbolic links.
//! - `ObjectArchive` is an object store which represents data as a flat mapping of keys to binary
//! blobs instead of a hierarchy of files.

#![allow(dead_code)]

pub use relative_path;

pub use file::{Entry, EntryMetadata, EntryType, FileArchive};
pub use object::{
    Checksum, Compression, Encryption, HashAlgorithm, Key, KeySalt, Object, ObjectArchive,
    RepositoryConfig,
};

mod file;
mod object;
mod store;
