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

#![cfg(feature = "store-sftp")]

use std::fmt::{self, Debug};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use bitflags::_core::fmt::Formatter;
use ssh2::{self, RenameFlags, Sftp};
use uuid::Uuid;

use super::common::{DataStore, OpenStore};
use crate::store::OpenOption;

// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "fc299876-c5ff-11ea-ada1-8b0ec1509cde";

// The names of files in the data store.
const BLOCKS_DIRECTORY: &str = "blocks";
const STAGING_DIRECTORY: &str = "stage";
const VERSION_FILE: &str = "version";

/// The configuration for an `SftpStore`.
pub struct SftpConfig {
    /// The `Sftp` object representing an open connection to the SSH server.
    pub sftp: Sftp,

    /// The path relative to the SFTP root to connect to.
    pub path: PathBuf,
}

/// A `DataStore` which stores data on an SFTP server.
///
/// The `store-sftp` cargo feature is required to use this.
pub struct SftpStore {
    sftp: Sftp,
    path: PathBuf,
}

impl SftpStore {
    /// Create a new `SftpStore`.
    fn create_new(sftp: Sftp, path: PathBuf) -> crate::Result<Self> {
        // Create the directories in the data store.
        sftp.mkdir(&path, 0o755)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        sftp.mkdir(&path.join(BLOCKS_DIRECTORY), 0o755)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        sftp.mkdir(&path.join(STAGING_DIRECTORY), 0o755)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        // Write the version ID file.
        let mut version_file = sftp
            .create(&path.join(VERSION_FILE))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        version_file.write_all(CURRENT_VERSION.as_bytes())?;

        Ok(SftpStore { sftp, path })
    }

    /// Open an existing `SftpStore`.
    fn open_existing(sftp: Sftp, path: PathBuf) -> crate::Result<Self> {
        // Read the version ID file.
        let mut version_file = sftp
            .open(&path.join(VERSION_FILE))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        let mut version_id = String::new();
        version_file.read_to_string(&mut version_id)?;

        // Verify the version ID.
        if version_id != CURRENT_VERSION {
            return Err(crate::Error::UnsupportedFormat);
        }

        Ok(SftpStore { sftp, path })
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

    /// Return whether the given remote `path` exists.
    fn exists(&self, path: &Path) -> bool {
        self.sftp.stat(path).is_ok()
    }
}

impl OpenStore for SftpStore {
    type Config = SftpConfig;

    fn open(config: Self::Config, options: OpenOption) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let SftpConfig { sftp, path } = config;
        let stats = sftp
            .stat(&path)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let exists = stats.is_file()
            || (stats.is_dir()
                && !sftp
                    .readdir(&path)
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
                    .is_empty());

        if options.contains(OpenOption::CREATE_NEW) {
            if exists {
                Err(crate::Error::AlreadyExists)
            } else {
                Self::create_new(sftp, path)
            }
        } else if options.contains(OpenOption::CREATE) && !exists {
            Self::create_new(sftp, path)
        } else {
            if !exists {
                return Err(crate::Error::NotFound);
            }

            let store = Self::open_existing(sftp, path)?;

            if options.contains(OpenOption::TRUNCATE) {
                let block_directories = store
                    .sftp
                    .readdir(&store.path.join(BLOCKS_DIRECTORY))
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                for (block_directory, _) in block_directories {
                    let blocks = store
                        .sftp
                        .readdir(&block_directory)
                        .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                    for (block_path, _) in blocks {
                        store
                            .sftp
                            .unlink(&block_path)
                            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                    }
                }
            }

            Ok(store)
        }
    }
}

impl DataStore for SftpStore {
    type Error = io::Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let staging_path = self.staging_path(id);
        let block_path = self.block_path(id);

        // If this is the first block its sub-directory, the directory needs to be created.
        let parent = block_path.parent().unwrap();
        if !self.exists(&parent) {
            self.sftp.mkdir(&parent, 0o755)?;
        }

        // Write to a staging file and then atomically move it to its final destination.
        let mut staging_file = self.sftp.create(&staging_path)?;
        staging_file.write_all(data)?;
        self.sftp.rename(
            &staging_path,
            &block_path,
            Some(RenameFlags::ATOMIC | RenameFlags::OVERWRITE),
        )?;

        // Remove any unused staging files.
        for (path, _) in self.sftp.readdir(&self.path.join(STAGING_DIRECTORY))? {
            self.sftp.unlink(&path)?;
        }

        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let block_path = self.block_path(id);

        if !self.exists(&block_path) {
            return Ok(None);
        }

        let mut file = self.sftp.open(&block_path)?;

        let mut buffer = Vec::with_capacity(file.stat()?.size.unwrap_or(0) as usize);
        file.read_to_end(&mut buffer)?;
        Ok(Some(buffer))
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let block_path = self.block_path(id);

        if !self.exists(&block_path) {
            return Ok(());
        }

        self.sftp.unlink(&block_path)?;

        Ok(())
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        let block_directories = self.sftp.readdir(&self.path.join(BLOCKS_DIRECTORY))?;
        let mut block_ids = Vec::new();

        for (block_directory, _) in block_directories {
            for (block_path, _) in self.sftp.readdir(&block_directory)? {
                let file_name = block_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .expect("Block file name is invalid.");
                let id = Uuid::parse_str(file_name).expect("Block file name is invalid.");
                block_ids.push(id);
            }
        }

        Ok(block_ids)
    }
}

impl Debug for SftpStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SftpStore {{ path: {:?} }}", self.path)
    }
}
