/*
 * Copyright 2019 Garrett Powell
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
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::object::{ChunkHash, ObjectHandle};

/// A type which can be used as a key in an `ObjectRepository`.
pub trait Key: Eq + Hash + Clone + Serialize + DeserializeOwned {}

impl<T> Key for T where T: Eq + Hash + Clone + Serialize + DeserializeOwned {}

/// The header for an `ObjectRepository`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Header<K: Eq + Hash> {
    /// A map of chunk hashes to the IDs of those chunks.
    pub chunks: HashMap<ChunkHash, Uuid>,

    /// A map of object keys to information about those objects.
    pub objects: HashMap<K, ObjectHandle>,
}

impl<K: Key> Default for Header<K> {
    fn default() -> Self {
        Header {
            chunks: HashMap::new(),
            objects: HashMap::new(),
        }
    }
}

impl<K: Key> Header<K> {
    /// Remove chunks not referenced by any object from the header.
    pub fn clean_chunks(&mut self) {
        let referenced_chunks = self
            .objects
            .values()
            .flat_map(|object| &object.chunks)
            .map(|chunk| chunk.hash)
            .collect::<HashSet<_>>();

        self.chunks
            .retain(|hash, _| referenced_chunks.contains(hash));
    }
}
