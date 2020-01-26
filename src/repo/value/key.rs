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

use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

/// A `Key` with an associated value type.
#[derive(Debug, PartialEq, Eq)]
pub struct ValueKey<K, V> {
    key: K,
    value: PhantomData<V>,
}

impl<K, V> ValueKey<K, V> {
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

/// A type of data stored in the `ObjectRepository` which backs a `ValueRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum KeyType<K> {
    /// A serialized value.
    Data(K),

    /// The current repository version.
    Version,
}
