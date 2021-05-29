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
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::id_table::{IdTable, UniqueId};
use crate::repo::state_repo::{Restore as StateRestore, StateKeys};

pub const STATE_KEYS: StateKeys<ValueRepoKey> = StateKeys {
    current: ValueRepoKey::CurrentState,
    previous: ValueRepoKey::PreviousState,
    temp: ValueRepoKey::TempState,
};

/// The state for a `ValueRepo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueRepoState<K: Eq + Hash> {
    pub key_table: HashMap<K, UniqueId>,
    pub id_table: IdTable,
}

impl<K: Eq + Hash> ValueRepoState<K> {
    /// Return a new empty `ContentRepoState`.
    pub fn new() -> Self {
        ValueRepoState {
            key_table: HashMap::new(),
            id_table: IdTable::new(),
        }
    }

    /// Clear the `ContentRepoState` in place.
    pub fn clear(&mut self) {
        self.key_table.clear();
        self.id_table = IdTable::new();
    }
}

/// The key for the `KeyRepo` which backs a `ValueRepo`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum ValueRepoKey {
    /// The object which contains the serialized current repository state.
    CurrentState,

    /// The object which contains the serialized repository state as of the previous commit.
    PreviousState,

    /// The object which is used to temporarily store the object state.
    TempState,

    /// An object which stores a serialized value.
    Value(UniqueId),
}

/// An in-progress operation to restore a [`ValueRepo`] to a [`Savepoint`].
///
/// See [`Restore`] for details.
///
/// [`ValueRepo`]: crate::repo::value::ValueRepo
/// [`Savepoint`]: crate::repo::Savepoint
/// [`Restore`]: crate::repo::key::Restore
#[derive(Debug, Clone)]
pub struct Restore<K: Eq + Hash>(pub(super) StateRestore<ValueRepoKey, ValueRepoState<K>>);

impl<K: Eq + Hash> Restore<K> {
    /// Return whether the savepoint used to start this restore is valid.
    pub fn is_valid(&self) -> bool {
        self.0.is_valid()
    }

    /// The ID of the repository instance this `Restore` is associated with.
    pub fn instance(&self) -> Uuid {
        self.0.instance()
    }
}
