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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::repo::id_table::{IdTable, UniqueId};
use crate::repo::state_repo::{Restore as StateRestore, StateKeys};

use super::HashAlgorithm;

/// The default hash algorithm to use for `ContentRepo`.
const DEFAULT_ALGORITHM: HashAlgorithm = HashAlgorithm::Blake3;

pub const STATE_KEYS: StateKeys<ContentRepoKey> = StateKeys {
    current: ContentRepoKey::CurrentState,
    previous: ContentRepoKey::PreviousState,
    temp: ContentRepoKey::TempState,
};

/// The state for a `ContentRepo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentRepoState {
    /// A map of content hashes to IDs for the objects which store the contents.
    pub hash_table: HashMap<Vec<u8>, UniqueId>,

    /// The table which allocates unique object IDs.
    pub id_table: IdTable,

    /// The currently selected hash algorithm.
    pub hash_algorithm: HashAlgorithm,
}

impl ContentRepoState {
    /// Return a new empty `ContentRepoState`.
    pub fn new() -> Self {
        ContentRepoState {
            hash_table: HashMap::new(),
            id_table: IdTable::new(),
            hash_algorithm: DEFAULT_ALGORITHM,
        }
    }

    /// Clear the `ContentRepoState` in place.
    pub fn clear(&mut self) {
        self.hash_table.clear();
        self.id_table = IdTable::new();
    }
}

/// The key for the `KeyRepo` which backs a `ContentRepo`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum ContentRepoKey {
    /// The object which contains the serialized current repository state.
    CurrentState,

    /// The object which contains the serialized repository state as of the previous commit.
    PreviousState,

    /// The object which is used to temporarily store the object state.
    TempState,

    /// The object we write the data to before the hash is fully calculated.
    Stage,

    /// An object which stores data by its cryptographic hash.
    Object(UniqueId),
}

/// An in-progress operation to restore a [`ContentRepo`] to a [`Savepoint`].
///
/// See [`Restore`] for details.
///
/// [`ContentRepo`]: crate::repo::content::ContentRepo
/// [`Savepoint`]: crate::repo::Savepoint
/// [`Restore`]: crate::repo::key::Restore
#[derive(Debug, Clone)]
pub struct Restore<'a>(pub(super) StateRestore<'a, ContentRepoKey, ContentRepoState>);

impl<'a> Restore<'a> {
    /// Return whether the savepoint used to start this restore is valid.
    pub fn is_valid(&self) -> bool {
        self.0.is_valid()
    }
}
