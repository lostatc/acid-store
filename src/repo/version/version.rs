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

use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::repo::ContentId;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Version {
    pub(super) id: usize,
    pub(super) created: SystemTime,
    pub(super) size: u64,
    pub(super) content_id: ContentId,
}

impl Version {
    /// A number that uniquely identifies this version among versions of the same key.
    ///
    /// This number starts at 1 and increases by 1 with each version.
    pub fn id(&self) -> usize {
        self.id
    }

    /// The time this version was created.
    pub fn created(&self) -> SystemTime {
        self.created
    }

    /// The size of this version in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// A value representing the contents of this version.
    pub fn content_id(&self) -> ContentId {
        self.content_id
    }
}

/// The key to use in the `ObjectRepository` which backs a `VersionRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum VersionKey<K> {
    /// The list of versions of a given key.
    Index(K),

    /// The current version of a given key.
    Object(K),

    /// The version of a given key with a given index.
    Version(K, usize),

    /// The current version of the repository.
    RepositoryVersion,
}
