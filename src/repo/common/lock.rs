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

use std::hash::Hash;
use std::sync::{Arc, Weak};

use weak_table::WeakHashSet;

/// A lock acquired on a resource.
///
/// The lock is released when this value is dropped.
#[derive(Debug)]
pub struct Lock<T>(Arc<T>);

/// A value which keeps track of locks on resources identified by generic IDs.
///
/// This locks resources between threads in a process using weak references.
#[derive(Debug)]
pub struct LockTable<T: Eq + Hash>(WeakHashSet<Weak<T>>);

impl<T: Eq + Hash> LockTable<T> {
    /// Create a new empty `LockTable`.
    pub fn new() -> Self {
        Self(WeakHashSet::new())
    }

    /// Attempt to acquire a lock on the given `id`.
    ///
    /// This returns a new lock or `None` if the resource is already locked.
    pub fn acquire_lock(&mut self, id: T) -> Option<Lock<T>> {
        if self.0.contains(&id) {
            None
        } else {
            let id_arc = Arc::from(id);
            self.0.insert(Arc::clone(&id_arc));
            Some(Lock(id_arc))
        }
    }
}
