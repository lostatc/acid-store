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

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::config::RepoConfig;
use super::encryption::KeySalt;
use super::id_table::IdTable;
use super::object::{Chunk, ObjectHandle};
use super::state::{ChunkInfo, PackIndex};

/// The repository state which is persisted to the data store on each commit.
#[derive(Debug, Serialize, Deserialize)]
pub struct Header {
    /// The map of chunks to information about them.
    pub chunks: HashMap<Chunk, ChunkInfo>,

    /// A map of block IDs to their locations in packs.
    pub packs: HashMap<Uuid, Vec<PackIndex>>,

    /// The map of managed objects to object handles for each instance ID.
    pub managed: HashMap<Uuid, HashMap<Uuid, ObjectHandle>>,

    /// The table of object handle IDs.
    pub handle_table: IdTable,
}

/// Metadata for a repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoMetadata {
    /// The unique ID of this repository.
    pub id: Uuid,

    /// The configuration for the repository.
    pub config: RepoConfig,

    /// The master encryption key encrypted with the user's password.
    pub master_key: Vec<u8>,

    /// The salt used to derive a key from the user's password.
    pub salt: KeySalt,

    /// The ID of the chunk which stores the repository header.
    pub header_id: Uuid,
}

impl RepoMetadata {
    /// Create a `RepoInfo` using the metadata in this struct.
    pub fn to_info(&self) -> RepoInfo {
        RepoInfo {
            id: self.id,
            config: self.config.clone(),
        }
    }
}

/// Information about a repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoInfo {
    id: Uuid,
    config: RepoConfig,
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
}
