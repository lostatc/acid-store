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
pub type KeyTable<K> = HashMap<K, KeyId>;
