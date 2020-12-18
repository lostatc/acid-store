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

use hex_literal::hex;
use once_cell::sync::Lazy;
use rmp_serde::{from_read, to_vec};
use secrecy::ExposeSecret;
use uuid::Uuid;

use crate::store::{DataStore, OpenStore};

use super::chunking::Chunking;
use super::compression::Compression;
use super::config::RepoConfig;
use super::convert::ConvertRepo;
use super::encryption::{Encryption, EncryptionKey, KeySalt, ResourceLimit};
use super::id_table::IdTable;
use super::lock::LockTable;
use super::metadata::{peek_info_store, Header, RepoMetadata};
use super::packing::Packing;
use super::repository::{ObjectRepo, METADATA_BLOCK_ID, VERSION_BLOCK_ID};
use super::state::RepoState;

/// The default repository instance ID.
///
/// This is the instance ID that is used by [`OpenOptions`] when an instance isn't specified.
///
/// [`OpenOptions`]: crate::repo::OpenOptions
pub const DEFAULT_INSTANCE: Uuid = Uuid::from_bytes(hex!("ea978302 bfd8 11ea b92b 031a9ad75c07"));

/// The current repository format version ID.
///
/// This must be changed any time a backwards-incompatible change is made to the repository
/// format.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("9070207d 98de 462f 91b8 68e73680ee18"));

/// A table of locks on repositories.
static REPO_LOCKS: Lazy<Mutex<LockTable>> = Lazy::new(|| Mutex::new(LockTable::new()));

/// The mode to use to open a repository.
#[derive(Debug, Clone, Copy)]
pub enum OpenMode {
    /// Open an existing repository, failing if it doesn't exist.
    Open,

    /// Open an existing repository or create a new one if it doesn't exist.
    Create,

    /// Create a new repository, failing if it already exists.
    CreateNew,
}

/// Open or create a repository.
///
/// This type is a builder used to open or create repositories. Typically, when using `OpenOptions`,
/// you'll first call [`new`], then chain method calls to configure how the repository will be
/// opened, and then finally call [`open`].
///
/// To open or create a repository, you'll need a value which implements [`OpenStore`] to pass to
/// [`open`]. You can think of this value as the configuration necessary to open the backing data
/// store. This builder can be used to open or create any repository type which implements
/// [`ConvertRepo`].
///
/// # Examples
/// ```no_run
/// # #[cfg(feature = "store-directory")] {
/// use acid_store::repo::{OpenOptions, OpenMode, key::KeyRepo, Chunking, Compression, Encryption, Packing};
/// use acid_store::store::DirectoryConfig;
///
/// let store_config = DirectoryConfig { path: "/path/to/store".into() };
/// let mut repo: KeyRepo<String> = OpenOptions::new()
///     .chunking(Chunking::Zpaq { bits: 18 })
///     .compression(Compression::Lz4 { level: 1 })
///     .encryption(Encryption::XChaCha20Poly1305)
///     .packing(Packing::Fixed(1024 * 16))
///     .password(b"password")
///     .mode(OpenMode::Create)
///     .open(&store_config)
///     .unwrap();
/// # }
/// ```
/// ```no_run
/// # #[cfg(feature = "store-directory")] {
/// use acid_store::repo::{OpenOptions, OpenMode, key::KeyRepo, Chunking, Compression, Encryption, Packing, RepoConfig};
/// use acid_store::store::DirectoryConfig;
///
/// let mut repo_config = RepoConfig::default();
/// repo_config.chunking = Chunking::Zpaq { bits: 18 };
/// repo_config.compression = Compression::Lz4 { level: 1 };
/// repo_config.encryption = Encryption::XChaCha20Poly1305;
/// repo_config.packing = Packing::Fixed(1024 * 16);
///
/// let store_config = DirectoryConfig { path: "/path/to/store".into() };
/// let mut repo: KeyRepo<String> = OpenOptions::new()
///     .config(repo_config)
///     .password(b"password")
///     .mode(OpenMode::Create)
///     .open(&store_config)
///     .unwrap();
/// # }
/// ```
///
/// [`new`]: crate::repo::OpenOptions::new
/// [`open`]: crate::repo::OpenOptions::open
/// [`OpenStore`]: crate::store::OpenStore
/// [`ConvertRepo`]: crate::repo::ConvertRepo
pub struct OpenOptions {
    config: RepoConfig,
    mode: OpenMode,
    password: Option<Vec<u8>>,
    instance: Uuid,
}

impl OpenOptions {
    /// Create a new `OpenOptions`.
    pub fn new() -> Self {
        Self {
            config: RepoConfig::default(),
            mode: OpenMode::Open,
            password: None,
            instance: DEFAULT_INSTANCE,
        }
    }

    /// The mode to use to open the repository.
    ///
    /// If this is not specified, the default mode is `OpenMode::Open`.
    pub fn mode(&mut self, mode: OpenMode) -> &mut Self {
        self.mode = mode;
        self
    }

    /// Use the given `config` instead of the default `RepoConfig`.
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    pub fn config(&mut self, config: RepoConfig) -> &mut Self {
        self.config = config;
        self
    }

    /// Overwrite the chunking method specified in [`RepoConfig::chunking`].
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    ///
    /// [`RepoConfig::chunking`]: crate::repo::RepoConfig::chunking
    pub fn chunking(&mut self, method: Chunking) -> &mut Self {
        self.config.chunking = method;
        self
    }

    /// Overwrite the packing method specified in [`RepoConfig::packing`].
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    ///
    /// [`RepoConfig::packing`]: crate::repo::RepoConfig::packing
    pub fn packing(&mut self, method: Packing) -> &mut Self {
        self.config.packing = method;
        self
    }

    /// Overwrite the compression method specified in [`RepoConfig::compression`].
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    ///
    /// [`RepoConfig::compression`]: crate::repo::RepoConfig::compression
    pub fn compression(&mut self, method: Compression) -> &mut Self {
        self.config.compression = method;
        self
    }

    /// Overwrite the encryption method specified in [`RepoConfig::encryption`].
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    ///
    /// [`RepoConfig::encryption`]: crate::repo::RepoConfig::encryption
    pub fn encryption(&mut self, method: Encryption) -> &mut Self {
        self.config.encryption = method;
        self
    }

    /// Overwrite the memory limit method specified in [`RepoConfig::memory_limit`].
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    ///
    /// [`RepoConfig::memory_limit`]: crate::repo::RepoConfig::memory_limit
    pub fn memory_limit(&mut self, limit: ResourceLimit) -> &mut Self {
        self.config.memory_limit = limit;
        self
    }

    /// Overwrite the operations limit method specified in [`RepoConfig::operations_limit`].
    ///
    /// This is only applicable when creating a new repository. This is ignored when opening an
    /// existing repository.
    ///
    /// [`RepoConfig::operations_limit`]: crate::repo::RepoConfig::operations_limit
    pub fn operations_limit(&mut self, limit: ResourceLimit) -> &mut Self {
        self.config.operations_limit = limit;
        self
    }

    /// Use the given `password`.
    ///
    /// This is required when encryption is enabled for the repository.
    pub fn password(&mut self, password: &[u8]) -> &mut Self {
        self.password = Some(password.to_vec());
        self
    }

    /// Open the instance of the repository with the given `id`.
    ///
    /// Opening a repository without specifying an instance ID will always open the same default
    /// instance. The ID of this default instance is [`DEFAULT_INSTANCE`].
    ///
    /// See the module-level documentation for [`crate::repo`] for more information on repository
    /// instances.
    ///
    /// [`DEFAULT_INSTANCE`]: crate::repo::DEFAULT_INSTANCE
    pub fn instance(&mut self, id: Uuid) -> &mut Self {
        self.instance = id;
        self
    }

    /// Open the repository, failing if it doesn't exist.
    fn open_repo(&self, mut store: impl DataStore + 'static) -> crate::Result<ObjectRepo> {
        // Acquire a lock on the repository.
        let repository_id = peek_info_store(&mut store)?.id();
        let lock = REPO_LOCKS
            .lock()
            .unwrap()
            .acquire_lock(repository_id)
            .ok_or(crate::Error::Locked)?;

        // Read the repository version to see if this is a compatible repository.
        let serialized_version = store
            .read_block(VERSION_BLOCK_ID)
            .map_err(|error| crate::Error::Store(error))?
            .ok_or(crate::Error::NotFound)?;
        let version =
            Uuid::from_slice(serialized_version.as_slice()).map_err(|_| crate::Error::Corrupt)?;
        if version != VERSION_ID {
            return Err(crate::Error::UnsupportedRepo);
        }

        // We read the metadata again after reading the UUID to prevent a race condition when
        // acquiring the lock.
        let serialized_metadata = store
            .read_block(METADATA_BLOCK_ID)
            .map_err(|error| crate::Error::Store(error))?
            .ok_or(crate::Error::Corrupt)?;
        let metadata: RepoMetadata =
            from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        let password = match self.password.clone() {
            Some(password) if metadata.config.encryption != Encryption::None => Some(password),
            // Return an error if a password was required but not provided.
            None if metadata.config.encryption != Encryption::None => {
                return Err(crate::Error::Password)
            }
            _ => None,
        };

        // Decrypt the master key for the repository.
        let master_key = match password {
            Some(password_bytes) => {
                let user_key = EncryptionKey::derive(
                    password_bytes.as_slice(),
                    &metadata.salt,
                    metadata.config.encryption.key_size(),
                    metadata.config.memory_limit,
                    metadata.config.operations_limit,
                );
                EncryptionKey::new(
                    metadata
                        .config
                        .encryption
                        .decrypt(&metadata.master_key, &user_key)
                        .map_err(|_| crate::Error::Password)?,
                )
            }
            None => EncryptionKey::new(Vec::new()),
        };

        // Read, decrypt, decompress, and deserialize the repository header.
        let encrypted_header = store
            .read_block(metadata.header_id)
            .map_err(|error| crate::Error::Store(error))?
            .ok_or(crate::Error::Corrupt)?;
        let compressed_header = metadata
            .config
            .encryption
            .decrypt(&encrypted_header, &master_key)
            .map_err(|_| crate::Error::Corrupt)?;
        let serialized_header = metadata
            .config
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
            store: Mutex::new(Box::new(store)),
            metadata,
            chunks,
            packs,
            master_key,
            lock,
        };

        let mut repository = ObjectRepo {
            state,
            instance_id: self.instance,
            managed,
            handle_table,
        };

        // Clean the repository in case changes were rolled back.
        repository.clean()?;

        Ok(repository)
    }

    /// Create a new repository, failing if one already exists.
    fn create_repo(&self, mut store: impl DataStore + 'static) -> crate::Result<ObjectRepo> {
        let password = match self.password.clone() {
            Some(password) if self.config.encryption != Encryption::None => Some(password),
            // Return an error if a password was required but not provided.
            None if self.config.encryption != Encryption::None => {
                return Err(crate::Error::Password)
            }
            _ => None,
        };

        // Acquire an exclusive lock on the repository.
        let id = Uuid::new_v4();
        let lock = REPO_LOCKS
            .lock()
            .unwrap()
            .acquire_lock(id)
            .ok_or(crate::Error::AlreadyExists)?;

        // Check if the repository already exists.
        if store
            .read_block(VERSION_BLOCK_ID)
            .map_err(|error| crate::Error::Store(error))?
            .is_some()
        {
            return Err(crate::Error::AlreadyExists);
        }

        // Generate the master encryption key.
        let master_key = match password {
            Some(..) => EncryptionKey::generate(self.config.encryption.key_size()),
            None => EncryptionKey::new(Vec::new()),
        };

        let salt = match password {
            Some(..) => KeySalt::generate(),
            None => KeySalt::empty(),
        };

        // Encrypt the master encryption key.
        let encrypted_master_key = match password {
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
        store
            .write_block(header_id, &encrypted_header)
            .map_err(|error| crate::Error::Store(error))?;

        // Create the repository metadata with the header block references.
        let metadata = RepoMetadata {
            id,
            config: self.config.clone(),
            master_key: encrypted_master_key,
            salt,
            header_id,
        };

        // Write the repository metadata.
        let serialized_metadata = to_vec(&metadata).expect("Could not serialize metadata.");
        store
            .write_block(METADATA_BLOCK_ID, &serialized_metadata)
            .map_err(|error| crate::Error::Store(error))?;

        // Write the repository version. We do this last because this signifies that the repository
        // is done being created.
        store
            .write_block(VERSION_BLOCK_ID, VERSION_ID.as_bytes())
            .map_err(|error| crate::Error::Store(error))?;

        let Header {
            chunks,
            packs,
            managed,
            handle_table,
        } = header;

        let state = RepoState {
            store: Mutex::new(Box::new(store)),
            metadata,
            chunks,
            packs,
            master_key,
            lock,
        };

        let mut repository = ObjectRepo {
            state,
            instance_id: self.instance,
            managed,
            handle_table,
        };

        Ok(repository)
    }

    /// Open or create the repository.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no repository in the data store and `OpenMode::Open` was
    /// specified.
    /// - `Error::AlreadyExists`: A repository already exists in the data store and
    /// `OpenMode::CreateNew` was specified.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Locked`: The repository is locked.
    /// - `Error::Password`: The password provided is invalid.
    /// - `Error::Password`: A password was required but not provided.
    /// - `Error::Deserialize`: Could not deserialize some data in the repository.
    /// - `Error::UnsupportedRepo`: The repository is an unsupported format. This can happen if the
    /// serialized data format changed or if the data store already contains a different type of
    /// repository.
    /// - `Error::UnsupportedStore`: The data store is an unsupported format. This can happen if
    /// the serialized data format changed or if the storage represented by `config` does not
    /// contain a valid data store.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn open<R, C>(&self, config: &C) -> crate::Result<R>
    where
        R: ConvertRepo,
        C: OpenStore,
    {
        let mut store = config.open()?;

        let repo = match self.mode {
            OpenMode::Open => self.open_repo(store)?,
            OpenMode::Create => {
                if store
                    .read_block(VERSION_BLOCK_ID)
                    .map_err(|error| crate::Error::Store(error))?
                    .is_some()
                {
                    self.open_repo(store)?
                } else {
                    self.create_repo(store)?
                }
            }
            OpenMode::CreateNew => self.create_repo(store)?,
        };

        R::from_repo(repo)
    }
}
