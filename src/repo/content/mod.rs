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

//! A content-addressable storage.
//!
//! This module contains the [`ContentRepo`] repository type.
//!
//! This is a repository which allows for accessing data by its cryptographic hash. See
//! [`HashAlgorithm`] for a list of supported hash algorithms. The default hash algorithm is BLAKE3,
//! but this can be changed using [`ContentRepo::change_algorithm`] once the repository is created.
//!
//! Like other repositories, changes made to the repository are not persisted to the data store
//! until [`Commit::commit`] is called. For details about deduplication, compression,
//! encryption, and locking, see the module-level documentation for [`crate::repo`].
//!
//! [`ContentRepo`]: crate::repo::content::ContentRepo
//! [`HashAlgorithm`]: crate::repo::content::HashAlgorithm
//! [`ContentRepo::change_algorithm`]: crate::repo::content::ContentRepo::change_algorithm
//! [`Commit::commit`]: crate::repo::Commit::commit
pub use hash::HashAlgorithm;
pub use iter::Hashes;
pub use repository::ContentRepo;

mod hash;
mod iter;
mod repository;
