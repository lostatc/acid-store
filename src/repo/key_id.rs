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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{Read, Write};

use rmp_serde::{from_read, to_vec};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::{Key, Object, ReadOnlyObject};
use crate::store::DataStore;

/// A unique `Copy`-able value which is associated with a key in a repository.
///
/// The purpose of this type is to be used in repositories which are implemented using an
/// `ObjectRepository` with a complex key type. If the key type is a struct or an enum which accepts
/// a `Key` value, that value will need to be cloned so that it can be moved into the struct or
/// enum. This type is meant to be used in place of `Key` values (which may be expensive to clone)
/// in complex keys.
///
/// A `KeyTable` can be used to associate each `Key` value with its corresponding `KeyId`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct KeyId(Uuid);

impl KeyId {
    /// Create a new unique `KeyId`.
    pub fn new() -> Self {
        KeyId(Uuid::new_v4())
    }
}

/// A table which maps `Key` values to `KeyId` values.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct KeyTable<K: Key>(HashMap<K, KeyId>);

impl<K: Key> KeyTable<K> {
    /// Create a new empty key table.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Return whether the table contains the given `key`.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.0.contains_key(key)
    }

    /// Insert the given `key` into the table and return its `KeyId`.
    ///
    /// If the given `key` already exists, it is replaced.
    pub fn insert(&mut self, key: K) -> KeyId {
        let id = KeyId::new();
        self.0.insert(key, id);
        id
    }

    /// Remove the given `key` from the table and return its `KeyId`.
    pub fn remove<Q>(&mut self, key: &Q) -> Option<KeyId>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.0.remove(key)
    }

    /// Get the `KeyId` associated with the given `key` from the table.
    pub fn get<Q>(&self, key: &Q) -> Option<KeyId>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.0.get(key).copied()
    }

    /// Return an iterator of the keys in the table.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.0.keys()
    }

    /// Return an iterator of the keys and key IDs in the table.
    pub fn iter(&self) -> impl Iterator<Item = (&K, KeyId)> {
        self.0.iter().map(|(key, key_id)| (key, *key_id))
    }

    /// Serialize this table and write it to the given `object`.
    pub fn write<R: Key, S: DataStore>(&self, mut object: Object<R, S>) -> crate::Result<()> {
        object.serialize(&self.0)
    }

    /// Read and deserialize a table from the given `object` and return it.
    pub fn read<R: Key, S: DataStore>(mut object: ReadOnlyObject<R, S>) -> crate::Result<Self> {
        Ok(KeyTable(object.deserialize()?))
    }
}
