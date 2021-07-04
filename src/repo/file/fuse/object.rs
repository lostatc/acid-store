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

use std::collections::{hash_map::Entry as HashMapEntry, HashMap};

use crate::repo::Object;

#[derive(Debug)]
pub struct ObjectTable(HashMap<u64, Object>);

impl ObjectTable {
    /// Return a new empty `ObjectTable`.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Return an `Object` for the file at the given `inode`.
    ///
    /// The returned object may have a transaction in progress.
    pub fn open(&mut self, inode: u64, default: Object) -> &mut Object {
        self.0.entry(inode).or_insert(default)
    }

    /// Commit changes to the `Object` for the file at the given `inode` if it is open.
    ///
    /// If the object is not open, this returns `Ok`.
    pub fn commit(&mut self, inode: u64) -> crate::Result<()> {
        if let Some(object) = self.0.get_mut(&inode) {
            object.commit()?;
        }
        Ok(())
    }

    /// Return an `Object` for the file at the given `inode`.
    ///
    /// This commits changes if the object was already open to ensure there is not a transaction in
    /// progress when this method returns.
    pub fn open_commit(&mut self, inode: u64, default: Object) -> crate::Result<&mut Object> {
        match self.0.entry(inode) {
            HashMapEntry::Occupied(entry) => {
                let object = entry.into_mut();
                object.commit()?;
                Ok(object)
            }
            HashMapEntry::Vacant(entry) => Ok(entry.insert(default)),
        }
    }

    /// Close the object for the file at the given `inode` if it is open.
    pub fn close(&mut self, inode: u64) -> bool {
        self.0.remove(&inode).is_some()
    }
}
