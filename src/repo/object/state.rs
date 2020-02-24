/*
 * Copyright 2019-2020 Garrett Powell
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

use std::cell::{Ref, RefCell};
use std::fmt::Debug;
use std::rc::Rc;

use crate::repo::Key;
use crate::store::DataStore;

use super::encryption::EncryptionKey;
use super::header::Header;
use super::lock::Lock;
use super::metadata::RepositoryMetadata;

/// The state associated with an `ObjectRepository`.
#[derive(Debug)]
pub struct ObjectState<K: Key, S: DataStore> {
    /// The data store which backs this repository.
    pub store: S,

    /// The metadata for the repository.
    pub metadata: RepositoryMetadata,

    /// The repository's header.
    pub header: Header<K>,

    /// The master encryption key for the repository.
    pub master_key: EncryptionKey,

    /// The lock on the repository.
    pub lock: Lock,
}
