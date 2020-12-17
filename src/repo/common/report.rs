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

use std::collections::{HashMap, HashSet};

use once_cell::sync::Lazy;
use uuid::Uuid;

use super::object::{ChunkHash, ObjectHandle};

/// An empty set of managed object IDs
static EMPTY_SET: Lazy<HashSet<Uuid>> = Lazy::new(|| HashSet::new());

/// A report of the integrity of the data in an `ObjectRepo`.
#[derive(Debug)]
pub struct IntegrityReport {
    /// The hashes of chunks which are corrupt.
    pub(super) corrupt_chunks: HashSet<ChunkHash>,

    /// The IDs of managed objects which are corrupt.
    pub(super) corrupt_managed: HashMap<Uuid, HashSet<Uuid>>,
}

impl IntegrityReport {
    /// Returns whether there is any corrupt data in any instance of the repository.
    pub fn is_corrupt(&self) -> bool {
        !self.corrupt_chunks.is_empty()
    }

    /// Returns whether the object associated with `handle` is valid (not corrupt).
    ///
    /// The given `handle` can be from any instance of the repository.
    pub fn check_unmanaged(&self, handle: &ObjectHandle) -> bool {
        if self.corrupt_chunks.is_empty() {
            // If there are no corrupt chunks, the object can't be corrupt.
            return true;
        }
        for chunk in &handle.chunks {
            // If any one of the object's chunks is corrupt, the object is corrupt.
            if self.corrupt_chunks.contains(&chunk.hash) {
                return false;
            }
        }
        true
    }

    /// Return the set of managed objects which are corrupt.
    ///
    /// This returns the set of IDs of managed objects from the given `instance` which are corrupt.
    pub fn list_managed(&self, instance: Uuid) -> &HashSet<Uuid> {
        self.corrupt_managed.get(&instance).unwrap_or(&EMPTY_SET)
    }
}
