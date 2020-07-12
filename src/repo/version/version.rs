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

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::repo::object::ObjectHandle;
use crate::repo::ContentId;
use std::collections::BTreeMap;
use std::io::Read;

/// Information about a version in a `VersionRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Version {
    pub(super) id: u32,
    pub(super) created: SystemTime,
    pub(super) content_id: ContentId,
}

impl Version {
    /// A number that uniquely identifies this version among versions of the same key.
    ///
    /// This number starts at 1 and increases by 1 with each version.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// The time this version was created.
    pub fn created(&self) -> SystemTime {
        self.created
    }

    /// Return a `ContentId` representing the contents of this version.
    ///
    /// See `ObjectHandle::content_id` for details.
    pub fn content_id(&self) -> &ContentId {
        &self.content_id
    }

    /// Return the size of the contents of the version in bytes.
    pub fn size(&self) -> u64 {
        self.content_id.size()
    }

    /// Return whether this version has the same contents as `other`.
    ///
    /// See `ContentId::compare_contents` for details.
    pub fn compare_contents(&self, other: impl Read) -> crate::Result<bool> {
        self.content_id.compare_contents(other)
    }
}

/// Information with a version.
#[derive(Debug, Serialize, Deserialize)]
pub struct VersionInfo {
    /// The time the version was created.
    pub(super) created: SystemTime,

    /// The handle of the object which contains the contents of the version.
    pub(super) handle: ObjectHandle,
}

/// Information associated with each key.
#[derive(Debug, Serialize, Deserialize)]
pub struct KeyInfo {
    /// The map of versions of this key.
    pub versions: BTreeMap<u32, VersionInfo>,

    /// The handle of the object which contains the current contents.
    pub object: ObjectHandle,
}
