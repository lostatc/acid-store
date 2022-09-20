#![cfg(feature = "store-directory")]

use std::fs::{create_dir_all, read_dir, remove_file, rename, File};
use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::anyhow;
use uuid::Uuid;

use super::data_store::{BlockId, BlockKey, BlockType, DataStore};
use super::open_store::OpenStore;

/// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "9ab66f8a-f883-11eb-b994-734187b3c515";

// The names of top-level files in the data store.
const STORE_DIRECTORY: &str = "store";
const STAGING_DIRECTORY: &str = "stage";
const VERSION_FILE: &str = "version";

fn type_path(kind: BlockType) -> PathBuf {
    match kind {
        BlockType::Data => [STORE_DIRECTORY, "data"].iter().collect(),
        BlockType::Lock => [STORE_DIRECTORY, "locks"].iter().collect(),
        BlockType::Header => [STORE_DIRECTORY, "headers"].iter().collect(),
    }
}

fn block_path(key: BlockKey) -> PathBuf {
    match key {
        BlockKey::Data(id) => {
            let uuid_str = id.as_ref().as_hyphenated().to_string();
            type_path(BlockType::Data)
                .join(&uuid_str[..2])
                .join(&uuid_str)
        }
        BlockKey::Lock(id) => {
            let uuid_str = id.as_ref().as_hyphenated().to_string();
            type_path(BlockType::Lock).join(&uuid_str)
        }
        BlockKey::Header(id) => {
            let uuid_str = id.as_ref().as_hyphenated().to_string();
            type_path(BlockType::Header).join(&uuid_str)
        }
        BlockKey::Super => [STORE_DIRECTORY, "super"].iter().collect(),
        BlockKey::Version => [STORE_DIRECTORY, "version"].iter().collect(),
    }
}

/// The configuration for opening a [`DirectoryStore`].
///
/// [`DirectoryStore`]: crate::store::DirectoryStore
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-directory")))]
pub struct DirectoryConfig {
    /// The path of the directory store.
    pub path: PathBuf,
}

impl OpenStore for DirectoryConfig {
    type Store = DirectoryStore;

    fn open(&self) -> crate::Result<Self::Store> {
        create_dir_all(&self.path)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        create_dir_all(self.path.join(STORE_DIRECTORY))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        create_dir_all(self.path.join(STAGING_DIRECTORY))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        create_dir_all(self.path.join(type_path(BlockType::Data)))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        create_dir_all(self.path.join(type_path(BlockType::Lock)))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        create_dir_all(self.path.join(type_path(BlockType::Header)))
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let version_path = self.path.join(VERSION_FILE);

        if version_path.exists() {
            // Read the version ID file.
            let mut version_file = File::open(&version_path)
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            let mut version_id = String::new();
            version_file.read_to_string(&mut version_id)?;

            // Verify the version ID.
            if version_id != CURRENT_VERSION {
                return Err(crate::Error::UnsupportedStore);
            }
        } else {
            // Write the version ID file.
            let mut version_file = File::create(&version_path)
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            version_file.write_all(CURRENT_VERSION.as_bytes())?;
        }

        Ok(DirectoryStore {
            path: self.path.clone(),
        })
    }
}

/// A `DataStore` which stores data in a directory in the local file system.
///
/// You can use [`DirectoryConfig`] to open a data store of this type.
///
/// [`DirectoryConfig`]: crate::store::DirectoryConfig
#[derive(Debug)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-directory")))]
pub struct DirectoryStore {
    /// The path of the store's root directory.
    path: PathBuf,
}

impl DirectoryStore {
    /// Return the path where a block with the given `key` will be stored.
    fn block_path(&self, key: BlockKey) -> PathBuf {
        self.path.join(block_path(key))
    }

    /// Return a new staging path.
    fn staging_path(&self) -> PathBuf {
        let uuid_str = Uuid::new_v4().as_hyphenated().to_string();
        self.path.join(STAGING_DIRECTORY).join(&uuid_str)
    }
}

impl DataStore for DirectoryStore {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        let staging_path = self.staging_path();
        let block_path = self.block_path(key);

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

    fn read_block(&mut self, key: BlockKey) -> anyhow::Result<Option<Vec<u8>>> {
        let block_path = self.block_path(key);

        if block_path.exists() {
            let mut file = File::open(block_path)?;
            let mut buffer = Vec::with_capacity(file.metadata()?.len() as usize);
            file.read_to_end(&mut buffer)?;
            Ok(Some(buffer))
        } else {
            Ok(None)
        }
    }

    fn remove_block(&mut self, key: BlockKey) -> anyhow::Result<()> {
        let block_path = self.block_path(key);

        if block_path.exists() {
            remove_file(self.block_path(key))?;
        }

        Ok(())
    }

    fn list_blocks(&mut self, kind: BlockType) -> anyhow::Result<Vec<BlockId>> {
        let mut block_ids = Vec::new();

        match kind {
            BlockType::Data => {
                for directory_entry in read_dir(self.path.join(type_path(kind)))? {
                    for block_entry in read_dir(directory_entry?.path())? {
                        let file_name = block_entry?.file_name();
                        let id = Uuid::parse_str(
                            file_name
                                .to_str()
                                .ok_or_else(|| anyhow!("Block file name is invalid."))?,
                        )
                        .map_err(|_| anyhow!("Block file name is invalid."))?
                        .into();
                        block_ids.push(id);
                    }
                }
            }
            BlockType::Lock | BlockType::Header => {
                for block_entry in read_dir(self.path.join(type_path(kind)))? {
                    let file_name = block_entry?.file_name();
                    let id = Uuid::parse_str(
                        file_name
                            .to_str()
                            .ok_or_else(|| anyhow!("Block file name is invalid."))?,
                    )
                    .map_err(|_| anyhow!("Block file name is invalid."))?
                    .into();
                    block_ids.push(id);
                }
            }
        }

        Ok(block_ids)
    }
}
