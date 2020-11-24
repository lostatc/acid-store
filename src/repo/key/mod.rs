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

//! An object store which maps keys to seekable binary blobs.
//!
//! This module contains the [`KeyRepo`] repository type.
//!
//! A [`KeyRepo`] maps keys to seekable binary blobs called objects and stores them persistently in
//! a [`DataStore`]. A key is any type which implements `Key`.
//!
//! Like other repositories, changes made to the repository are not persisted to the data store
//! until [`KeyRepo::commit`] is called. For details about deduplication, compression, encryption, and
//! locking, see the module-level documentation for [`crate::repo`].
//!
//! [`KeyRepo`]: crate::repo::key::KeyRepo
//! [`DataStore`]: crate::store::DataStore
//! [`KeyRepo::commit`]: crate::repo::key::KeyRepo::commit

pub use self::repository::{Key, KeyRepo};

mod repository;
