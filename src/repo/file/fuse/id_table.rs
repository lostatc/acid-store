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

use std::collections::HashSet;

/// A table for allocating `UniqueId` values.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct IdTable {
    /// The highest used ID value (the high water mark).
    highest: u64,

    /// A set of unused ID values below the high water mark.
    unused: HashSet<u64>,

    /// The set of ID values which cannot ever be allocated.
    reserved: HashSet<u64>,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub fn new() -> Self {
        Self {
            highest: 0,
            unused: HashSet::new(),
            reserved: HashSet::new(),
        }
    }

    /// Return a new empty `IdTable` with the given `reserved` IDs.
    pub fn with_reserved(reserved: impl IntoIterator<Item = u64>) -> Self {
        Self {
            highest: 0,
            unused: HashSet::new(),
            reserved: reserved.into_iter().collect::<HashSet<_>>(),
        }
    }

    /// Return the next unused ID from the table.
    pub fn next(&mut self) -> u64 {
        match self.unused.iter().next().copied() {
            Some(id) => {
                self.unused.remove(&id);
                id
            }
            None => {
                self.highest += 1;
                while self.reserved.contains(&self.highest) {
                    self.highest += 1;
                }
                self.highest
            }
        }
    }

    /// Return whether the given `id` is in the table.
    pub fn contains(&self, id: u64) -> bool {
        id <= self.highest && !self.unused.contains(&id)
    }

    /// Return the given `id` back to the table.
    ///
    /// This returns `true` if the value was returned or `false` if it was unused or reserved.
    pub fn recycle(&mut self, id: u64) -> bool {
        if !self.contains(id) || self.reserved.contains(&id) {
            return false;
        }
        self.unused.insert(id);
        true
    }
}
