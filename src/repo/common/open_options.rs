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
use std::sync::Mutex;
use std::time::SystemTime;

use hex_literal::hex;
use lazy_static::lazy_static;
use rmp_serde::{from_read, to_vec};
use uuid::Uuid;

use super::config::RepositoryConfig;
use super::convert::ConvertRepo;
use super::encryption::{Encryption, EncryptionKey, KeySalt};
use super::id_table::IdTable;
use super::lock::{LockStrategy, LockTable};
use super::metadata::{Header, RepositoryMetadata};
use super::object::ObjectHandle;
use super::repository::{ObjectRepository, METADATA_BLOCK_ID, VERSION_BLOCK_ID};
use super::state::RepositoryState;
use crate::repo::{Chunking, Compression, ResourceLimit};
use crate::store::DataStore;

/// The instance to use when an instance isn't supplied.
const GLOBAL_INSTANCE: Uuid = Uuid::from_bytes(hex!("ea978302 bfd8 11ea b92b 031a9ad75c07"));

/// The current repository format version ID.
///
/// This must be changed any time a backwards-incompatible change is made to the repository
/// format.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("17597ef8 bce7 11ea b70b 17210b172c53"));

lazy_static! {
    /// A table of locks on repositories.
    static ref REPO_LOCKS: Mutex<LockTable> = Mutex::new(LockTable::new());
}

/// Open or create a repository from a `DataStore`.
///
/// `OpenOptions` can be used to open any repository type which implements `ConvertRepo`.
pub struct OpenOptions<S: DataStore> {
    store: S,
    config: RepositoryConfig,
    locking: LockStrategy,
    password: Option<Vec<u8>>,
    instance: Uuid,
}

impl<S: DataStore> OpenOptions<S> {
    /// Create a new `OpenOptions` for opening or creating a repository backed by `store`.
    pub fn new(store: S) -> Self {
        Self {
            store,
            config: RepositoryConfig::default(),
            locking: LockStrategy::Abort,
            password: None,
            instance: GLOBAL_INSTANCE,
        }
    }

    /// Use the given `config` instead of the default `RepositoryConfig`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn config(mut self, config: RepositoryConfig) -> Self {
        self.config = config;
        self
    }

    /// Overwrite the chunking method specified in `RepositoryConfig::chunking`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn chunking(mut self, method: Chunking) -> Self {
        self.config.chunking = method;
        self
    }

    /// Overwrite the compression method specified in `RepositoryConfig::compression`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn compression(mut self, method: Compression) -> Self {
        self.config.compression = method;
        self
    }

    /// Overwrite the encryption method specified in `RepositoryConfig::encryption`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn encryption(mut self, method: Encryption) -> Self {
        self.config.encryption = method;
        self
    }

    /// Overwrite the memory limit method specified in `RepositoryConfig::memory_limit`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn memory_limit(mut self, limit: ResourceLimit) -> Self {
        self.config.memory_limit = limit;
        self
    }

    /// Overwrite the operations limit method specified in `RepositoryConfig::operations_limit`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn operations_limit(mut self, limit: ResourceLimit) -> Self {
        self.config.operations_limit = limit;
        self
    }

    /// Use the given locking `strategy` instead of `LockStrategy::Abort`.
    ///
    /// This is only applicable when opening an existing repository. This is ignored when creating
    /// a new repository.
    pub fn locking(mut self, strategy: LockStrategy) -> Self {
        self.locking = strategy;
        self
    }

    /// Use the given `password`.
    ///
    /// This is required when encryption is enabled for the repository.
    pub fn password(mut self, password: &[u8]) -> Self {
        self.password = Some(password.to_vec());
        self
    }

    /// Open the instance of the repository with the given `id` instead of the global instance.
    ///
    /// Opening a repository without specifying an instance ID will always open the same global
    /// instance.
    ///
    /// See `ObjectRepository` for details.
    pub fn instance(mut self, id: Uuid) -> Self {
        self.instance = id;
        self
    }

    /// Open the repository, failing if it doesn't exist.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no repository in the given data store.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Locked`: The repository is locked and `LockStrategy::Abort` was used.
    /// - `Error::Password`: The password provided is invalid.
    /// - `Error::Password` A password was required but not provided or provided but not required.
    /// - `Error::UnsupportedFormat`: The backing is an unsupported format. This can happen if the
    /// serialized data format changed or if the data store already contains a different type of
    /// repository.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn open<R>(mut self) -> crate::Result<R>
    where
        R: ConvertRepo<S>,
    {
        // Acquire a lock on the repository.
        let repository_id = ObjectRepository::peek_info(&mut self.store)?.id();
        let lock = REPO_LOCKS
            .lock()
            .unwrap()
            .acquire_lock(repository_id, self.locking)?;

        // Read the repository version to see if this is a compatible repository.
        let serialized_version = self
            .store
            .read_block(VERSION_BLOCK_ID)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .ok_or(crate::Error::NotFound)?;
        let version =
            Uuid::from_slice(serialized_version.as_slice()).map_err(|_| crate::Error::Corrupt)?;
        if version != VERSION_ID {
            return Err(crate::Error::UnsupportedFormat);
        }

        // We read the metadata again after reading the UUID to prevent a race condition when
        // acquiring the lock.
        let serialized_metadata = self
            .store
            .read_block(METADATA_BLOCK_ID)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .ok_or(crate::Error::Corrupt)?;
        let metadata: RepositoryMetadata =
            from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // Return an error if a password was required but not provided.
        if self.password.is_none() && metadata.encryption != Encryption::None {
            return Err(crate::Error::Password);
        }

        // Return an error if a password was provided but not required.
        if self.password.is_some() && metadata.encryption == Encryption::None {
            return Err(crate::Error::Password);
        }

        // Decrypt the master key for the repository.
        let master_key = match self.password {
            Some(password_bytes) => {
                let user_key = EncryptionKey::derive(
                    password_bytes.as_slice(),
                    &metadata.salt,
                    metadata.encryption.key_size(),
                    metadata.memory_limit,
                    metadata.operations_limit,
                );
                EncryptionKey::new(
                    metadata
                        .encryption
                        .decrypt(&metadata.master_key, &user_key)
                        .map_err(|_| crate::Error::Password)?,
                )
            }
            None => EncryptionKey::new(Vec::new()),
        };

        // Read, decrypt, decompress, and deserialize the chunk map.
        let encrypted_chunks = self
            .store
            .read_block(metadata.header.chunks)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .ok_or(crate::Error::Corrupt)?;
        let compressed_chunks = metadata
            .encryption
            .decrypt(&encrypted_chunks, &master_key)
            .map_err(|_| crate::Error::Corrupt)?;
        let serialized_chunks = metadata
            .compression
            .decompress(&compressed_chunks)
            .map_err(|_| crate::Error::Corrupt)?;
        let chunks = from_read(serialized_chunks.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // Read, decrypt, decompress, and deserialize the managed object map.
        let encrypted_managed = self
            .store
            .read_block(metadata.header.managed)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .ok_or(crate::Error::Corrupt)?;
        let compressed_managed = metadata
            .encryption
            .decrypt(&encrypted_managed, &master_key)
            .map_err(|_| crate::Error::Corrupt)?;
        let serialized_managed = metadata
            .compression
            .decompress(&compressed_managed)
            .map_err(|_| crate::Error::Corrupt)?;
        let mut managed: HashMap<Uuid, HashMap<Uuid, ObjectHandle>> =
            from_read(serialized_managed.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // Read, decrypt, decompress, and deserialize the handle ID table.
        let encrypted_table = self
            .store
            .read_block(metadata.header.handles)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .ok_or(crate::Error::Corrupt)?;
        let compressed_table = metadata
            .encryption
            .decrypt(&encrypted_table, &master_key)
            .map_err(|_| crate::Error::Corrupt)?;
        let serialized_table = metadata
            .compression
            .decompress(&compressed_table)
            .map_err(|_| crate::Error::Corrupt)?;
        let handle_table =
            from_read(serialized_table.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // If the given instance ID is not in the managed object map, add it.
        managed.entry(self.instance).or_insert_with(HashMap::new);

        let state = RepositoryState {
            store: Mutex::new(self.store),
            metadata,
            chunks,
            master_key,
            lock,
        };

        let repository = ObjectRepository {
            state,
            instance_id: self.instance,
            managed,
            handle_table,
        };

        R::from_repo(repository)
    }

    /// Create a new repository, failing if one already exists.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: A repository already exists in the given `store`.
    /// - `Error::Password` A password was required but not provided or provided but not required.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create_new<R>(mut self) -> crate::Result<R>
    where
        R: ConvertRepo<S>,
    {
        // Return an error if a password was required but not provided.
        if self.password.is_none() && self.config.encryption != Encryption::None {
            return Err(crate::Error::Password);
        }

        // Return an error if a password was provided but not required.
        if self.password.is_some() && self.config.encryption == Encryption::None {
            return Err(crate::Error::Password);
        }

        // Acquire an exclusive lock on the repository.
        let id = Uuid::new_v4();
        let lock = REPO_LOCKS
            .lock()
            .unwrap()
            .acquire_lock(id, LockStrategy::Abort)
            .map_err(|_| crate::Error::AlreadyExists)?;

        // Check if the repository already exists.
        if self
            .store
            .read_block(VERSION_BLOCK_ID)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .is_some()
        {
            return Err(crate::Error::AlreadyExists);
        }

        // Generate the master encryption key.
        let master_key = match self.password {
            Some(..) => EncryptionKey::generate(self.config.encryption.key_size()),
            None => EncryptionKey::new(Vec::new()),
        };

        let salt = match self.password {
            Some(..) => KeySalt::generate(),
            None => KeySalt::empty(),
        };

        // Encrypt the master encryption key.
        let encrypted_master_key = match self.password {
            Some(password_bytes) => {
                let user_key = EncryptionKey::derive(
                    password_bytes.as_slice(),
                    &salt,
                    self.config.encryption.key_size(),
                    self.config.memory_limit,
                    self.config.operations_limit,
                );
                self.config
                    .encryption
                    .encrypt(master_key.as_ref(), &user_key)
            }
            None => Vec::new(),
        };

        // Generate and write the chunk map.
        let chunks = HashMap::new();
        let serialized_chunks = to_vec(&chunks).expect("Could not serialize the chunk map.");
        let compressed_chunks = self.config.compression.compress(&serialized_chunks)?;
        let encrypted_chunks = self
            .config
            .encryption
            .encrypt(&compressed_chunks, &master_key);
        let chunks_id = Uuid::new_v4();
        self.store
            .write_block(chunks_id, &encrypted_chunks)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        // Generate and write the managed object map.
        let mut managed = HashMap::new();
        managed.insert(self.instance, HashMap::new());
        let serialized_managed = to_vec(&managed).expect("Could not serialize the chunk map.");
        let compressed_managed = self.config.compression.compress(&serialized_managed)?;
        let encrypted_managed = self
            .config
            .encryption
            .encrypt(&compressed_managed, &master_key);
        let managed_id = Uuid::new_v4();
        self.store
            .write_block(managed_id, &encrypted_managed)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        // Generate and write the handle ID table.
        let handle_table = IdTable::new();
        let serialized_handles = to_vec(&handle_table).expect("Could not serialize the chunk map.");
        let compressed_handles = self.config.compression.compress(&serialized_handles)?;
        let encrypted_handles = self
            .config
            .encryption
            .encrypt(&compressed_handles, &master_key);
        let handles_id = Uuid::new_v4();
        self.store
            .write_block(handles_id, &encrypted_handles)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let header = Header {
            chunks: chunks_id,
            managed: managed_id,
            handles: handles_id,
        };

        // Create the repository metadata with the header block references.
        let metadata = RepositoryMetadata {
            id,
            chunking: self.config.chunking,
            compression: self.config.compression,
            encryption: self.config.encryption,
            memory_limit: self.config.memory_limit,
            operations_limit: self.config.operations_limit,
            master_key: encrypted_master_key,
            salt,
            header,
            creation_time: SystemTime::now(),
        };

        // Write the repository metadata.
        let serialized_metadata = to_vec(&metadata).expect("Could not serialize metadata.");
        self.store
            .write_block(METADATA_BLOCK_ID, &serialized_metadata)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        // Write the repository version. We do this last because this signifies that the repository
        // is done being created.
        self.store
            .write_block(VERSION_BLOCK_ID, VERSION_ID.as_bytes())
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let state = RepositoryState {
            store: Mutex::new(self.store),
            metadata,
            chunks,
            master_key,
            lock,
        };

        let repository = ObjectRepository {
            state,
            instance_id: self.instance,
            managed,
            handle_table,
        };

        R::from_repo(repository)
    }

    /// Open the repository if it exists or create one if it doesn't.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Locked`: The repository is locked and `LockStrategy::Abort` was used.
    /// - `Error::Password`: The password provided is invalid.
    /// - `Error::Password`: A password was required but not provided or provided but not required.
    /// - `Error::UnsupportedFormat`: The backing is an unsupported format. This can happen if the
    /// serialized data format changed or if the data store already contains a different type of
    /// repository.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create<R>(mut self) -> crate::Result<R>
    where
        R: ConvertRepo<S>,
    {
        if self
            .store
            .read_block(VERSION_BLOCK_ID)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .is_some()
        {
            self.open()
        } else {
            self.create_new()
        }
    }
}
