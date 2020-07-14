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

use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use uuid::Uuid;

use super::chunking::Chunking;
use super::compression::Compression;
use super::config::RepoConfig;
use super::encryption::{Encryption, KeySalt, ResourceLimit};

/// Chunk IDs for accessing persistent repository state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    /// The ID of the chunk which stores the map of chunks.
    pub chunks: Uuid,

    /// The ID of the chunk which stores the map of managed objects.
    pub managed: Uuid,

    /// The ID of the chunk which stores the table of ID handles.
    pub handles: Uuid,
}

/// Metadata for a repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoMetadata {
    /// The unique ID of this repository.
    pub id: Uuid,

    /// The chunking method being used in this repository.
    pub chunking: Chunking,

    /// The compression method being used in this repository.
    pub compression: Compression,

    /// The encryption method being used in this repository.
    pub encryption: Encryption,

    /// The maximum amount of memory the key derivation function will use in bytes.
    pub memory_limit: ResourceLimit,

    /// The maximum number of computations the key derivation function will perform.
    pub operations_limit: ResourceLimit,

    /// The master encryption key encrypted with the user's password.
    pub master_key: Vec<u8>,

    /// The salt used to derive a key from the user's password.
    pub salt: KeySalt,

    /// The IDs of chunks which store repository state.
    pub header: Header,

    /// The time this repository was created.
    pub creation_time: SystemTime,
}

impl RepoMetadata {
    /// Create a `RepoInfo` using the metadata in this struct.
    pub fn to_info(&self) -> RepoInfo {
        RepoInfo {
            id: self.id,
            config: RepoConfig {
                chunking: self.chunking.clone(),
                compression: self.compression.clone(),
                encryption: self.encryption.clone(),
                memory_limit: self.memory_limit,
                operations_limit: self.operations_limit,
            },
            created: self.creation_time,
        }
    }
}

/// Information about a repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoInfo {
    id: Uuid,
    config: RepoConfig,
    created: SystemTime,
}

impl RepoInfo {
    /// The unique ID for this repository.
    ///
    /// This ID is different from the instance ID; this ID is shared between all instances of a
    /// repository.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// The configuration used to create this repository.
    pub fn config(&self) -> &RepoConfig {
        &self.config
    }

    /// The time this repository was created.
    pub fn created(&self) -> SystemTime {
        self.created
    }
}
