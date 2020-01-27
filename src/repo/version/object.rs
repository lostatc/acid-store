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

use std::io::{self, Read, Seek, SeekFrom};

use crate::repo::{ContentId, Key, Object};
use crate::store::DataStore;

/// A handle for accessing data in a repository.
///
/// `ReadOnlyObject` wraps `Object` but does not implement `Write` or provide methods for modifying
/// the data.
pub struct ReadOnlyObject<'a, K: Key, S: DataStore> {
    object: Object<'a, K, S>,
}

impl<'a, K: Key, S: DataStore> ReadOnlyObject<'a, K, S> {
    /// Return the size of the object in bytes.
    pub fn size(&self) -> u64 {
        self.object.size()
    }

    /// Return a `ContentId` representing the contents of this object.
    ///
    /// The returned value represents the contents of the object at the time this method was called.
    pub fn content_id(&self) -> ContentId {
        self.object.content_id()
    }

    /// Verify the integrity of the data in this object.
    ///
    /// This returns `true` if the object is valid and `false` if it is corrupt.
    pub fn verify(&self) -> crate::Result<bool> {
        self.object.verify()
    }
}

impl<'a, K: Key, S: DataStore> Read for ReadOnlyObject<'a, K, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.object.read(buf)
    }
}

impl<'a, K: Key, S: DataStore> Seek for ReadOnlyObject<'a, K, S> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.object.seek(pos)
    }
}

impl<'a, K: Key, S: DataStore> From<Object<'a, K, S>> for ReadOnlyObject<'a, K, S> {
    fn from(object: Object<'a, K, S>) -> Self {
        Self { object }
    }
}
