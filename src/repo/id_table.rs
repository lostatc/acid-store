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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

/// An ID value which is unique within the same `IdTable`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UniqueId(u64);

/// A table for allocating `UniqueId` values.
#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct IdTable {
    /// The highest used ID value (the high water mark).
    highest: u64,

    /// A set of unused ID values below the high water mark.
    unused: HashSet<u64>,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the next unused ID from the table.
    pub fn next(&mut self) -> UniqueId {
        match self.unused.iter().next().copied() {
            Some(id) => {
                self.unused.remove(&id);
                UniqueId(id)
            }
            None => {
                self.highest += 1;
                UniqueId(self.highest)
            }
        }
    }

    /// Return whether the given `id` is in the table.
    pub fn contains(&self, id: UniqueId) -> bool {
        id.0 <= self.highest && !self.unused.contains(&id.0)
    }

    /// Return the given `id` back to the table.
    ///
    /// This returns `true` if the value was returned or `false` if it was unused.
    pub fn recycle(&mut self, id: UniqueId) -> bool {
        if !self.contains(id) {
            return false;
        }
        self.unused.insert(id.0);
        true
    }
}
