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

use serde::{Deserialize, Serialize};

use crate::repo::common::{IdTable as UniqueIdTable, UniqueId};

/// An opaque ID which uniquely identifies an object in a [`StateRepo`].
///
/// [`StateRepo`]: crate::repo::state::StateRepo
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObjectId(UniqueId);

/// A table for allocating `ObjectId` values.
#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct IdTable(UniqueIdTable);

impl IdTable {
    /// Return a new empty `IdTable`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the next unused ID from the table.
    pub fn next(&mut self) -> ObjectId {
        ObjectId(self.0.next())
    }

    /// Return whether the given `id` is in the table.
    pub fn contains(&self, id: ObjectId) -> bool {
        self.0.contains(id.0)
    }

    /// Return the given `id` back to the table.
    ///
    /// This returns `true` if the value was returned or `false` if it was unused.
    pub fn recycle(&mut self, id: ObjectId) -> bool {
        self.0.recycle(id.0)
    }
}
