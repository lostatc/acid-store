/*
 * Copyright 2019 Wren Powell
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

use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::repo::Key;

/// A `Key` with an associated value type.
#[derive(Debug, PartialEq, Eq)]
pub struct ValueKey<K: Key, V: Serialize + DeserializeOwned> {
    key: K,
    value: PhantomData<V>,
}

impl<K: Key, V: Serialize + DeserializeOwned> ValueKey<K, V> {
    /// Create a new `ValueKey` which wraps the given `key`.
    pub fn new(key: K) -> Self {
        Self {
            key,
            value: PhantomData,
        }
    }

    /// Return a reference to the wrapped key.
    pub fn get_ref(&self) -> &K {
        &self.key
    }

    /// Consume this value and return the wrapped key.
    pub fn into_inner(self) -> K {
        self.key
    }
}
