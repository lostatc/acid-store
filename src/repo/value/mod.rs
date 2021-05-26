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

//! A persistent, heterogeneous, map-like collection.
//!
//! This module contains the [`ValueRepo`] repository type.
//!
//! This is a repository which maps keys to concrete values instead of binary blobs. Values are
//! serialized and deserialized automatically using a space-efficient binary format.
//!
//! Like other repositories, changes made to the repository are not persisted to the data store
//! until [`ValueRepo::commit`] is called. For details about deduplication, compression, encryption,
//! and locking, see the module-level documentation for [`crate::repo`].
//!
//! [`ValueRepo`]: crate::repo::value::ValueRepo
//! [`ValueRepo::commit`]: crate::repo::value::ValueRepo::commit

pub use self::repository::ValueRepo;
pub use self::state::Restore;

mod repository;
mod state;
