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

//! `disk-archive` is a file format and library for efficiently storing large chunks of binary data.
//!
//! The `disk-archive` file format works similarly to archive formats like ZIP and TAR, with
//! some key differences. Files in a ZIP archive can't be updated in-place. New data can be written
//! to the archive, but the old data sticks around, taking up space. The only way to reclaim space
//! in a ZIP file is to unpack and repack the entire archive. The file format used by this crate
//! doesn't have this limitation, and it provides other benefits like content-defined block-level
//! deduplication.
//!
//! While ZIP and TAR files are meant to be portable, archives created by this crate can be used to
//! create high-performance file formats. They also support transparent compression and encryption.
//!
//! This crate provides two abstractions for interacting with with the archive format:
//!
//! - `FileArchive` is a file archive like ZIP or TAR which supports modification times, POSIX file
//! permissions, extended attributes, and symbolic links.
//! - `ObjectArchive` is an object store which represents data as a flat mapping of keys to binary
//! blobs instead of a hierarchy of files.

#![allow(dead_code)]

pub use object::{ArchiveConfig, Compression, Encryption, Key, KeySalt, Object, ObjectArchive};

mod error;
mod object;
