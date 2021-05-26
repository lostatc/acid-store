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

use serde::{Deserialize, Serialize};

use crate::repo::id_table::{IdTable, UniqueId};
use crate::repo::state_repo::{Restore as StateRestore, StateKeys};

use super::entry::EntryHandle;
use super::path_tree::PathTree;

pub const STATE_KEYS: StateKeys<FileRepoKey> = StateKeys {
    current: FileRepoKey::CurrentState,
    previous: FileRepoKey::PreviousState,
    temp: FileRepoKey::TempState,
};

/// The state for a `FileRepo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRepoState {
    /// A map of relative file paths to the handles of the objects containing their entries.
    pub path_table: PathTree<EntryHandle>,

    /// A table which allocates unique IDs for file contents and file metadata.
    pub id_table: IdTable,
}

impl FileRepoState {
    /// Return a new empty `FileRepoState`.
    pub fn new() -> Self {
        FileRepoState {
            path_table: PathTree::new(),
            id_table: IdTable::new(),
        }
    }

    /// Clear the `FileRepoState` in place.
    pub fn clear(&mut self) {
        self.path_table.clear();
        self.id_table = IdTable::new();
    }
}

/// The key for the `KeyRepo` which backs a `FileRepo`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum FileRepoKey {
    /// The object which contains the serialized current repository state.
    CurrentState,

    /// The object which contains the serialized repository state as of the previous commit.
    PreviousState,

    /// The object which is used to temporarily store the object state.
    TempState,

    /// The object which stores the contents of a file or a file's metadata.
    Object(UniqueId),
}

/// An in-progress operation to restore a [`FileRepo`] to a [`Savepoint`].
///
/// See [`Restore`] for details.
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`Savepoint`]: crate::repo::Savepoint
/// [`Restore`]: crate::repo::key::Restore
#[derive(Debug, Clone)]
pub struct Restore<'a>(pub(super) StateRestore<'a, FileRepoKey, FileRepoState>);

impl<'a> Restore<'a> {
    /// Return whether the savepoint used to start this restore is valid.
    pub fn is_valid(&self) -> bool {
        self.0.is_valid()
    }
}
