/*
 * Copyright 2019 Garrett Powell
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

//! `data-store` is a library for secure, deduplicated, transactional, and verifiable data storage.
//!
//! This crate provides high-level abstractions for data storage over a number of storage backends.
//! Storage backends are easy to implement, and this library builds on top of them to provide the
//! following features:
//! - Content-defined block-level deduplication
//! - Transparent encryption
//! - Transparent compression
//! - Automatic checksumming and integrity checking
//! - Atomicity, consistency, isolation, and durability (ACID)
//!
//! This library currently provides two abstractions for data storage:
//! - `ObjectRepository` is an object store which maps keys to binary blobs.
//! - `FileRepository` is a file archive like ZIP or TAR which supports modification times, POSIX
//! file permissions, extended attributes, and symbolic links.
//!
//! A repository stores its data in a `DataStore`, which is a small trait that can be implemented to
//! create new storage backends. The following data stores are provided out of the box:
//! - `DirectoryStore` stores data in a directory in the local file system.
//! - `MemoryStore` stores data in memory.

#![allow(dead_code)]

pub use relative_path;
pub use uuid;

pub use env::init;
pub use file::{Entry, EntryType, FileRepository};
pub use object::{
    Compression, Encryption, Key, LockStrategy, Object, ObjectRepository, RepositoryConfig,
    ResourceLimit,
};
pub use store::{DataStore, DirectoryStore, MemoryStore};

mod env;
mod file;
mod object;
mod store;
