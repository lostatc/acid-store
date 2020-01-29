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

use std::fs::{create_dir, create_dir_all, remove_dir_all, remove_file, rename, File};
use std::io::{self, Read, Write};
use std::path::PathBuf;

use uuid::Uuid;
use walkdir::WalkDir;

use super::common::DataStore;

/// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "2891c3da-297e-11ea-a7c9-1b8f8be4fc9b";

/// The names of files in the data store.
const BLOCKS_DIRECTORY: &str = "blocks";
const STAGING_DIRECTORY: &str = "stage";
const VERSION_FILE: &str = "version";

/// A `DataStore` which stores data in a directory in the local file system.
pub struct DirectoryStore {
    /// The path of the store's root directory.
    path: PathBuf,

    /// The path of the directory where blocks are stored.
    blocks_directory: PathBuf,

    /// The path of the directory were blocks are staged while being written to.
    staging_directory: PathBuf,
}

impl DirectoryStore {
    /// Create a new directory store at the given `path`.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: There is already a file at the given `path`.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create(path: PathBuf) -> crate::Result<Self> {
        if path.exists() {
            return Err(crate::Error::AlreadyExists);
        }

        // Create the files and directories in the data store.
        if let Some(parent_directory) = path.parent() {
            create_dir_all(parent_directory)?;
        }
        create_dir(&path)?;
        create_dir(&path.join(BLOCKS_DIRECTORY))?;

        // Write the version ID file.
        let mut version_file = File::create(&path.join(VERSION_FILE))?;
        version_file.write_all(CURRENT_VERSION.as_bytes())?;

        Self::open(path)
    }

    /// Open an existing directory store at `path`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is not a directory at `path`.
    /// - `Error::UnsupportedFormat`: There is not a `DirectoryStore` in the directory or it is an
    /// unsupported format.
    /// - `Error::Io`: An I/O error occurred.
    pub fn open(path: PathBuf) -> crate::Result<Self> {
        if !path.is_dir() {
            return Err(crate::Error::NotFound);
        }

        // Read the version ID file.
        let mut version_file = File::open(path.join(VERSION_FILE))?;
        let mut version_id = String::new();
        version_file.read_to_string(&mut version_id)?;

        // Verify the version ID.
        if version_id != CURRENT_VERSION {
            return Err(crate::Error::UnsupportedFormat);
        }

        Ok(DirectoryStore {
            path: path.clone(),
            blocks_directory: path.join(BLOCKS_DIRECTORY),
            staging_directory: path.join(STAGING_DIRECTORY),
        })
    }

    /// Return the path where a block with the given `id` will be stored.
    fn block_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.blocks_directory.join(&hex[..2]).join(hex)
    }

    /// Return the path where a block with the given `id` will be staged.
    fn staging_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.staging_directory.join(hex)
    }
}

impl DataStore for DirectoryStore {
    type Error = io::Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let staging_path = self.staging_path(id);
        let block_path = self.block_path(id);
        create_dir_all(staging_path.parent().unwrap())?;
        create_dir_all(block_path.parent().unwrap())?;

        // Write to a staging file and then atomically move it to its final destination.
        let mut staging_file = File::create(&staging_path)?;
        staging_file.write_all(data)?;
        rename(&staging_path, &block_path)?;

        // Remove any unused staging files.
        remove_dir_all(&self.staging_directory)?;

        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
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

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let block_path = self.block_path(id);

        if block_path.exists() {
            remove_file(self.block_path(id))
        } else {
            Ok(())
        }
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        // Collect the results into a vector so that we can release the lock on the data store.
        WalkDir::new(&self.blocks_directory)
            .min_depth(2)
            .into_iter()
            .map(|result| match result {
                Ok(entry) => Ok(Uuid::parse_str(
                    entry
                        .file_name()
                        .to_str()
                        .expect("Block file name is invalid."),
                )
                .expect("Block file name is invalid.")),
                Err(error) => Err(io::Error::from(error)),
            })
            .collect::<io::Result<Vec<_>>>()
    }
}
