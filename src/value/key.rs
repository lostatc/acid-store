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
use std::ops::{Deref, DerefMut};

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::Key;

/// A `Key` with an associated value type.
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

    /// Consume this value and return the wrapped key.
    pub fn into_inner(self) -> K {
        self.key
    }
}

impl<K: Key, V: Serialize + DeserializeOwned> Deref for ValueKey<K, V> {
    type Target = K;

    fn deref(&self) -> &Self::Target {
        &self.key
    }
}

impl<K: Key, V: Serialize + DeserializeOwned> DerefMut for ValueKey<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.key
    }
}
