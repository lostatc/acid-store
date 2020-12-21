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

use rmp_serde::from_read;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::common::repository::METADATA_BLOCK_ID;
use crate::store::{DataStore, OpenStore};

use super::config::RepoConfig;
use super::encryption::KeySalt;
use super::id_table::IdTable;
use super::object::{Chunk, ObjectHandle};
use super::state::{ChunkInfo, PackIndex};

/// The repository state which is persisted to the data store on each commit.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Return information about the repository in the given `store` without opening it.
pub fn peek_info_store(store: &mut impl DataStore) -> crate::Result<RepoInfo> {
    // Read and deserialize the metadata.
    let serialized_metadata = match store
        .read_block(METADATA_BLOCK_ID)
        .map_err(|error| crate::Error::Store(error))?
    {
        Some(data) => data,
        None => return Err(crate::Error::NotFound),
    };
    let metadata: RepoMetadata =
        from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

    Ok(metadata.to_info())
}

/// Return information about the repository in a data store without opening it.
///
/// This accepts the `config` used to open the data store.
///
/// # Errors
/// - `Error::NotFound`: There is no repository in the data store.
/// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
/// - `Error::UnsupportedStore`: The data store is an unsupported format. This can happen if
/// the serialized data format changed or if the storage represented by this value does not
/// contain a valid data store.
/// - `Error::Store`: An error occurred with the data store.
/// - `Error::Io`: An I/O error occurred.
pub fn peek_info(config: &impl OpenStore) -> crate::Result<RepoInfo> {
    // Open the data store.
    let mut store = config.open()?;
    peek_info_store(&mut store)
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
