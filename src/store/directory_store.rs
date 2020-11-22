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

#![cfg(feature = "store-directory")]

use std::fs::{create_dir_all, read_dir, remove_file, rename, File};
use std::io::{Read, Write};
use std::path::PathBuf;

use uuid::Uuid;

use super::common::DataStore;

/// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "2891c3da-297e-11ea-a7c9-1b8f8be4fc9b";

/// The names of files in the data store.
const BLOCKS_DIRECTORY: &str = "blocks";
const STAGING_DIRECTORY: &str = "stage";
const VERSION_FILE: &str = "version";

/// A `DataStore` which stores data in a directory in the local file system.
///
/// The `store-directory` cargo feature is required to use this.
#[derive(Debug)]
pub struct DirectoryStore {
    /// The path of the store's root directory.
    path: PathBuf,
}

impl DirectoryStore {
    /// Open or create a `DirectoryStore` in the given `path`.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `DirectoryStore` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn new(path: PathBuf) -> crate::Result<Self> {
        // Create the blocks directory in the data store.
        create_dir_all(&path).map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        create_dir_all(&path.join(BLOCKS_DIRECTORY))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        create_dir_all(&path.join(STAGING_DIRECTORY))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let version_path = path.join(VERSION_FILE);

        if version_path.exists() {
            // Read the version ID file.
            let mut version_file = File::open(&version_path)
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            let mut version_id = String::new();
            version_file.read_to_string(&mut version_id)?;

            // Verify the version ID.
            if version_id != CURRENT_VERSION {
                return Err(crate::Error::UnsupportedFormat);
            }
        } else {
            // Write the version ID file.
            let mut version_file = File::create(&version_path)
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            version_file.write_all(CURRENT_VERSION.as_bytes())?;
        }

        Ok(DirectoryStore { path })
    }

    /// Return the path where a block with the given `id` will be stored.
    fn block_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.path.join(BLOCKS_DIRECTORY).join(&hex[..2]).join(hex)
    }

    /// Return the path where a block with the given `id` will be staged.
    fn staging_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.path.join(STAGING_DIRECTORY).join(hex)
    }
}

impl DataStore for DirectoryStore {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> anyhow::Result<()> {
        let staging_path = self.staging_path(id);
        let block_path = self.block_path(id);

        // If this is the first block its sub-directory, the directory needs to be created.
        create_dir_all(&block_path.parent().unwrap())?;

        // Write to a staging file and then atomically move it to its final destination.
        let mut staging_file = File::create(&staging_path)?;
        staging_file.write_all(data)?;
        rename(&staging_path, &block_path)?;

        // Remove any unused staging files.
        for entry in read_dir(self.path.join(STAGING_DIRECTORY))? {
            remove_file(entry?.path())?;
        }

        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        let block_path = self.block_path(id);

        if block_path.exists() {
            let mut file = File::open(block_path)?;
            let mut buffer = Vec::with_capacity(file.metadata()?.len() as usize);
            file.read_to_end(&mut buffer)?;
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }

    fn remove_block(&mut self, id: Uuid) -> anyhow::Result<()> {
        let block_path = self.block_path(id);

        if block_path.exists() {
            remove_file(self.block_path(id))?;
        }

        Ok(())
    }

    fn list_blocks(&mut self) -> anyhow::Result<Vec<Uuid>> {
        let mut block_ids = Vec::new();

        for directory_entry in read_dir(self.path.join(BLOCKS_DIRECTORY))? {
            for block_entry in read_dir(directory_entry?.path())? {
                let file_name = block_entry?.file_name();
                let id = Uuid::parse_str(file_name.to_str().expect("Block file name is invalid."))
                    .expect("Block file name is invalid.");
                block_ids.push(id);
            }
        }

        Ok(block_ids)
    }
}
