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
use secrecy::ExposeSecret;
use uuid::Uuid;

use crate::repo::{Chunking, Compression, ResourceLimit};
use crate::store::DataStore;

use super::config::RepoConfig;
use super::convert::ConvertRepo;
use super::encryption::{Encryption, EncryptionKey, KeySalt};
use super::id_table::IdTable;
use super::lock::LockTable;
use super::metadata::{Header, RepoMetadata};
use super::packing::Packing;
use super::repository::{ObjectRepo, METADATA_BLOCK_ID, VERSION_BLOCK_ID};
use super::state::RepoState;

/// The instance to use when an instance isn't supplied.
const GLOBAL_INSTANCE: Uuid = Uuid::from_bytes(hex!("ea978302 bfd8 11ea b92b 031a9ad75c07"));

/// The current repository format version ID.
///
/// This must be changed any time a backwards-incompatible change is made to the repository
/// format.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("62c2b71d b8a7 454c a55a e4a47effb744"));

lazy_static! {
    /// A table of locks on repositories.
    static ref REPO_LOCKS: Mutex<LockTable> = Mutex::new(LockTable::new());
}

/// Open or create a repository from a `DataStore`.
///
/// `OpenOptions` can be used to open any repository type which implements `ConvertRepo`.
pub struct OpenOptions {
    store: Box<dyn DataStore + 'static>,
    config: RepoConfig,
    password: Option<Vec<u8>>,
    instance: Uuid,
}

impl OpenOptions {
    /// Create a new `OpenOptions` for opening or creating a repository backed by `store`.
    pub fn new(store: impl DataStore + 'static) -> Self {
        Self {
            store: Box::new(store),
            config: RepoConfig::default(),
            password: None,
            instance: GLOBAL_INSTANCE,
        }
    }

    /// Use the given `config` instead of the default `RepoConfig`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn config(mut self, config: RepoConfig) -> Self {
        self.config = config;
        self
    }

    /// Overwrite the chunking method specified in `RepoConfig::chunking`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn chunking(mut self, method: Chunking) -> Self {
        self.config.chunking = method;
        self
    }

    /// Overwrite the packing method specified in `RepoConfig::packing`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn packing(mut self, method: Packing) -> Self {
        self.config.packing = method;
        self
    }

    /// Overwrite the compression method specified in `RepoConfig::compression`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn compression(mut self, method: Compression) -> Self {
        self.config.compression = method;
        self
    }

    /// Overwrite the encryption method specified in `RepoConfig::encryption`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn encryption(mut self, method: Encryption) -> Self {
        self.config.encryption = method;
        self
    }

    /// Overwrite the memory limit method specified in `RepoConfig::memory_limit`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn memory_limit(mut self, limit: ResourceLimit) -> Self {
        self.config.memory_limit = limit;
        self
    }

    /// Overwrite the operations limit method specified in `RepoConfig::operations_limit`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn operations_limit(mut self, limit: ResourceLimit) -> Self {
        self.config.operations_limit = limit;
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
    /// See the module-level documentation for [`crate::repo`] for details.
    pub fn instance(mut self, id: Uuid) -> Self {
        self.instance = id;
        self
    }

    /// Open the repository, failing if it doesn't exist.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no repository in the given data store.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Locked`: The repository is locked.
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
        R: ConvertRepo,
    {
        // Acquire a lock on the repository.
        let repository_id = ObjectRepo::peek_info(&mut self.store)?.id();
        let lock = REPO_LOCKS
            .lock()
            .unwrap()
            .acquire_lock(repository_id)
            .ok_or(crate::Error::Locked)?;

        // Read the repository version to see if this is a compatible repository.
        let serialized_version = self
            .store
            .read_block(VERSION_BLOCK_ID)
            .map_err(|error| crate::Error::Store(error))?
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
            .map_err(|error| crate::Error::Store(error))?
            .ok_or(crate::Error::Corrupt)?;
        let metadata: RepoMetadata =
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

        // Read, decrypt, decompress, and deserialize the repository header.
        let encrypted_header = self
            .store
            .read_block(metadata.header_id)
            .map_err(|error| crate::Error::Store(error))?
            .ok_or(crate::Error::Corrupt)?;
        let compressed_header = metadata
            .encryption
            .decrypt(&encrypted_header, &master_key)
            .map_err(|_| crate::Error::Corrupt)?;
        let serialized_header = metadata
            .compression
            .decompress(&compressed_header)
            .map_err(|_| crate::Error::Corrupt)?;
        let header = from_read(serialized_header.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        let Header {
            chunks,
            packs,
            mut managed,
            handle_table,
        } = header;

        // If the given instance ID is not in the managed object map, add it.
        managed.entry(self.instance).or_insert_with(HashMap::new);

        let state = RepoState {
            store: Mutex::new(self.store),
            metadata,
            chunks,
            packs,
            read_buffer: None,
            write_buffer: None,
            master_key,
            lock,
        };

        let repository = ObjectRepo {
            state,
            instance_id: self.instance,
            managed,
            handle_table,
        };

        // Clean the repository in case changes were rolled back.
        repository.clean()?;

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
        R: ConvertRepo,
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
            .acquire_lock(id)
            .ok_or(crate::Error::AlreadyExists)?;

        // Check if the repository already exists.
        if self
            .store
            .read_block(VERSION_BLOCK_ID)
            .map_err(|error| crate::Error::Store(error))?
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
                    .encrypt(master_key.expose_secret(), &user_key)
            }
            None => Vec::new(),
        };

        // Generate the header.
        let mut managed = HashMap::new();
        managed.insert(self.instance, HashMap::new());
        let header = Header {
            chunks: HashMap::new(),
            packs: HashMap::new(),
            managed,
            handle_table: IdTable::new(),
        };

        // Serialize, encode, and write the header to the data store.
        let serialized_header =
            to_vec(&header).expect("Could not serialize the repository header.");
        let compressed_header = self.config.compression.compress(&serialized_header)?;
        let encrypted_header = self
            .config
            .encryption
            .encrypt(&compressed_header, &master_key);
        let header_id = Uuid::new_v4();
        self.store
            .write_block(header_id, &encrypted_header)
            .map_err(|error| crate::Error::Store(error))?;

        // Create the repository metadata with the header block references.
        let metadata = RepoMetadata {
            id,
            chunking: self.config.chunking,
            packing: self.config.packing,
            compression: self.config.compression,
            encryption: self.config.encryption,
            memory_limit: self.config.memory_limit,
            operations_limit: self.config.operations_limit,
            master_key: encrypted_master_key,
            salt,
            header_id,
            creation_time: SystemTime::now(),
        };

        // Write the repository metadata.
        let serialized_metadata = to_vec(&metadata).expect("Could not serialize metadata.");
        self.store
            .write_block(METADATA_BLOCK_ID, &serialized_metadata)
            .map_err(|error| crate::Error::Store(error))?;

        // Write the repository version. We do this last because this signifies that the repository
        // is done being created.
        self.store
            .write_block(VERSION_BLOCK_ID, VERSION_ID.as_bytes())
            .map_err(|error| crate::Error::Store(error))?;

        let Header {
            chunks,
            packs,
            managed,
            handle_table,
        } = header;

        let state = RepoState {
            store: Mutex::new(self.store),
            metadata,
            chunks,
            packs,
            write_buffer: None,
            read_buffer: None,
            master_key,
            lock,
        };

        let repository = ObjectRepo {
            state,
            instance_id: self.instance,
            managed,
            handle_table,
        };

        // Clean the repository in case changes were rolled back.
        repository.clean()?;

        R::from_repo(repository)
    }

    /// Open the repository if it exists or create one if it doesn't.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Locked`: The repository is locked.
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
        R: ConvertRepo,
    {
        if self
            .store
            .read_block(VERSION_BLOCK_ID)
            .map_err(|error| crate::Error::Store(error))?
            .is_some()
        {
            self.open()
        } else {
            self.create_new()
        }
    }
}
