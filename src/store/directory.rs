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

use std::collections::HashSet;
use std::fs::{create_dir, create_dir_all, File, remove_dir_all, remove_file, rename};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use std::sync::RwLock;

use fs2::FileExt;
use lazy_static::lazy_static;
use uuid::Uuid;
use walkdir::WalkDir;

use super::store::{ConcurrentDataStore, DataStore, LockStrategy};

/// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "2891c3da-297e-11ea-a7c9-1b8f8be4fc9b";

/// The names of files in the data store.
const BLOCKS_DIRECTORY: &str = "blocks";
const STAGING_DIRECTORY: &str = "stage";
const VERSION_FILE: &str = "version";
const LOCK_FILE: &str = "store.lock";

lazy_static! {
    /// The set of paths of directory stores which are currently open.
    static ref OPEN_STORES: RwLock<HashSet<PathBuf>> = RwLock::new(HashSet::new());
}

/// A `DataStore` which stores data in a directory in the local file system.
///
/// This data store protects against concurrent access by multiple processes using the operating
/// system's native file locking facilities. The program blocks until a lock can be acquired. A
/// data store of this type cannot be open more than once within the same process.
pub struct DirectoryStore {
    /// The path of the store's root directory.
    path: PathBuf,

    /// The path of the directory where blocks are stored.
    blocks_directory: PathBuf,

    /// The path of the directory were blocks are staged while being written to.
    staging_directory: PathBuf,

    /// The path of the data store's lock file.
    lock_file: File,
}

impl Drop for DirectoryStore {
    fn drop(&mut self) {
        // Remove this store from the set of open stores.
        OPEN_STORES.write().unwrap().remove(&self.path);
    }
}

impl DirectoryStore {
    /// Create a new directory store at the given `path`.
    ///
    /// # Errors
    /// - `ErrorKind::AlreadyExists`: There is already a file at the given path.
    /// - `ErrorKind::PermissionDenied`: The user lacks permissions to create the directory.
    pub fn create(path: PathBuf) -> io::Result<Self> {
        // Create the files and directories in the data store.
        if let Some(parent_directory) = path.parent() {
            create_dir_all(parent_directory)?;
        }
        create_dir(&path)?;
        create_dir(&path.join(BLOCKS_DIRECTORY))?;
        File::create(&path.join(LOCK_FILE))?;

        // Write the version ID file.
        let mut version_file = File::create(&path.join(VERSION_FILE))?;
        version_file.write_all(CURRENT_VERSION.as_bytes())?;

        Self::open(path, LockStrategy::Abort)
    }

    /// Open an existing directory store at `path`.
    ///
    /// # Errors
    /// - `ErrorKind::NotFound`: There is not a directory at `path`.
    /// - `ErrorKind::InvalidData`: The directory at `path` is not a valid directory store.
    /// - `ErrorKind::PermissionDenied`: The user lacks permissions to read the directory.
    /// - `ErrorKind::WouldBlock`: This store is already open in this process.
    /// - `ErrorKind::WouldBlock`: The store is locked and `LockStrategy::Abort` was used.
    pub fn open(path: PathBuf, strategy: LockStrategy) -> io::Result<Self> {
        let mut open_stores = OPEN_STORES.write().unwrap();
        let lock_file = File::create(path.join(LOCK_FILE))?;

        // Check if this store is already open in this process.
        if open_stores.contains(&path) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "This store is already open in this process.",
            ));
        } else {
            // Get an exclusive lock on the data store.
            match strategy {
                LockStrategy::Abort => lock_file.try_lock_exclusive()?,
                LockStrategy::Wait => lock_file.lock_exclusive()?
            };

            open_stores.insert(path.clone());
        }

        // Read the version ID file.
        let mut version_file = File::open(path.join(VERSION_FILE))?;
        let mut version_id = String::new();
        version_file.read_to_string(&mut version_id)?;

        // Verify the version ID.
        if version_id != CURRENT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "The directory is not a valid directory store.",
            ));
        }

        Ok(DirectoryStore {
            path: path.clone(),
            blocks_directory: path.join(BLOCKS_DIRECTORY),
            staging_directory: path.join(STAGING_DIRECTORY),
            lock_file
        })
    }

    /// Return the path where a block with the given `id` will be stored.
    fn block_path(&self, id: &Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.blocks_directory.join(&hex[..2]).join(hex)
    }

    /// Return the path where a block with the given `id` will be staged.
    fn staging_path(&self, id: &Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.staging_directory.join(hex)
    }
}

impl DataStore for DirectoryStore {
    fn write_block(&mut self, id: &Uuid, data: &[u8]) -> io::Result<()> {
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

    fn read_block(&self, id: &Uuid) -> io::Result<Vec<u8>> {
        let block_path = self.block_path(id);

        let buffer = if block_path.exists() {
            let mut file = File::open(block_path)?;
            let mut buffer = Vec::with_capacity(file.metadata()?.len() as usize);
            file.read_to_end(&mut buffer)?;
            buffer
        } else {
            panic!("There is no block with the given ID.");
        };

        Ok(buffer)
    }

    fn remove_block(&mut self, id: &Uuid) -> io::Result<()> {
        remove_file(self.block_path(id))
    }

    fn list_blocks(&self) -> io::Result<Vec<Uuid>> {
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

impl ConcurrentDataStore for DirectoryStore {}
