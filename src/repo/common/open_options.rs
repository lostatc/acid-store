/*
 * Copyright 2019-2021 Wren Powell
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
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex, RwLock};

use hex_literal::hex;
use rmp_serde::{from_read, to_vec};
use secrecy::ExposeSecret;
use uuid::Uuid;

use crate::store::{BlockKey, DataStore, OpenStore};

use super::chunking::Chunking;
use super::compression::Compression;
use super::config::RepoConfig;
use super::encryption::{Encryption, EncryptionKey, KeySalt, ResourceLimit};
use super::handle::HandleIdTable;
use super::lock::{lock_store, LockTable};
use super::metadata::{Header, RepoMetadata};
use super::open_repo::OpenRepo;
use super::packing::Packing;
use super::repository::KeyRepo;
use super::state::{InstanceId, RepoState};

/// The default repository instance ID.
///
/// This is the instance ID that is used by [`OpenOptions`] when an instance isn't specified.
///
/// [`OpenOptions`]: crate::repo::OpenOptions
pub const DEFAULT_INSTANCE: InstanceId = InstanceId::new(Uuid::from_bytes(hex!(
    "ea978302 bfd8 11ea b92b 031a9ad75c07"
)));

/// The current repository format version ID.
///
/// This must be changed any time a backwards-incompatible change is made to the repository
/// format.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("44253e72 f08f 11eb a2a3 a701701f8601"));

/// The mode to use to open a repository.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
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
/// [`OpenRepo`].
///
/// # Examples
/// ```
/// # #[cfg(all(feature = "encryption", feature = "compression"))] {
/// use acid_store::repo::{OpenOptions, OpenMode, key::KeyRepo, Chunking, Compression, Encryption, Packing};
/// use acid_store::store::MemoryConfig;
///
/// let store_config = MemoryConfig::new();
/// let mut repo: KeyRepo<String> = OpenOptions::new()
///     .chunking(Chunking::ZPAQ)
///     .compression(Compression::Lz4 { level: 1 })
///     .encryption(Encryption::XChaCha20Poly1305)
///     .packing(Packing::FIXED)
///     .password(b"password")
///     .mode(OpenMode::Create)
///     .open(&store_config)
///     .unwrap();
/// # }
/// ```
/// ```
/// # #[cfg(all(feature = "encryption", feature = "compression"))] {
/// use acid_store::repo::{OpenOptions, OpenMode, key::KeyRepo, Chunking, Compression, Encryption, Packing, RepoConfig};
/// use acid_store::store::MemoryConfig;
///
/// let mut repo_config = RepoConfig::default();
/// repo_config.chunking = Chunking::ZPAQ;
/// repo_config.compression = Compression::Lz4 { level: 1 };
/// repo_config.encryption = Encryption::XChaCha20Poly1305;
/// repo_config.packing = Packing::FIXED;
///
/// let store_config = MemoryConfig::new();
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
/// [`OpenRepo`]: crate::repo::OpenRepo
pub struct OpenOptions<'a> {
    config: RepoConfig,
    mode: OpenMode,
    password: Option<&'a [u8]>,
    instance: InstanceId,
    lock_context: &'a [u8],
    lock_handler: Box<dyn FnMut(&[u8]) -> bool + 'a>,
}

impl<'a> Default for OpenOptions<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> OpenOptions<'a> {
    /// Create a new `OpenOptions`.
    pub fn new() -> Self {
        Self {
            config: RepoConfig::default(),
            mode: OpenMode::Open,
            password: None,
            instance: DEFAULT_INSTANCE,
            lock_context: &[],
            lock_handler: Box::new(|_| false),
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
    pub fn password(&mut self, password: &'a [u8]) -> &mut Self {
        self.password = Some(password);
        self
    }

    /// Configure the behavior of repository locking.
    ///
    /// This method accepts a `context` which is associated with the lock on the repository once a
    /// lock is acquired. If a lock's context is not specified, the context value of the acquired
    /// lock will be empty. If encryption is enabled for the repository, the lock context is
    /// encrypted. You can change the context value of the held lock once the repository is open
    /// using [`Unlock::update_context`].
    ///
    /// This method also accepts a `handler` which is invoked if a lock is already held on the
    /// repository. This lock handler is passed the context value of the existing lock. If `handler`
    /// returns `true`, the existing lock will be removed and the repository will be opened. If
    /// `handler` returns `false`, the existing lock will be respected and opening the repository
    /// will fail. If a lock handler is not specified, an existing lock will always be respected.
    ///
    /// Opening a repository can still fail due to lock conflicts even if `handler` returns `true`
    /// or is never called.
    ///
    /// **Removing an existing lock is potentially dangerous, as concurrent access to a repository
    /// can cause data loss.**
    ///
    /// # Examples
    ///
    /// Always ignore any existing locks on the repository.
    ///
    /// ```
    /// # use acid_store::repo::{OpenOptions, OpenMode, key::KeyRepo};
    /// # use acid_store::store::MemoryConfig;
    /// let mut repo: KeyRepo<String> = OpenOptions::new()
    ///     .mode(OpenMode::Create)
    ///     .locking(&[], |_| true)
    ///     .open(&MemoryConfig::new())
    ///     .unwrap();
    /// ```
    ///
    /// [`Unlock::update_context`]: crate::repo::Unlock::update_context
    pub fn locking(
        &mut self,
        context: &'a [u8],
        handler: impl FnMut(&[u8]) -> bool + 'a,
    ) -> &mut Self {
        self.lock_context = context;
        self.lock_handler = Box::new(handler);
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
    pub fn instance(&mut self, id: InstanceId) -> &mut Self {
        self.instance = id;
        self
    }

    /// Open the repository, failing if it doesn't exist.
    fn open_repo<R: OpenRepo>(&mut self, mut store: impl DataStore + 'static) -> crate::Result<R> {
        // Read the repository version to see if this is a compatible repository.
        let serialized_version = store
            .read_block(BlockKey::Version)
            .map_err(crate::Error::Store)?
            .ok_or(crate::Error::NotFound)?;
        let version =
            Uuid::from_slice(serialized_version.as_slice()).map_err(|_| crate::Error::Corrupt)?;
        if version != VERSION_ID {
            return Err(crate::Error::UnsupportedRepo);
        }

        // Read the repository metadata from the super block.
        let serialized_metadata = store
            .read_block(BlockKey::Super)
            .map_err(crate::Error::Store)?
            .ok_or(crate::Error::Corrupt)?;
        let metadata: RepoMetadata =
            from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        let password = match self.password {
            Some(password) if metadata.config.encryption != Encryption::None => Some(password),
            // Return an error if a password was required but not provided.
            None if metadata.config.encryption != Encryption::None => {
                return Err(crate::Error::Password)
            }
            _ => None,
        };

        // Decrypt the master key for the repository.
        let master_key = match password {
            Some(password_bytes) => metadata.decrypt_master_key(&password_bytes)?,
            None => EncryptionKey::new(Vec::new()),
        };

        // Attempt to acquire a lock on the repository.
        let lock_id = lock_store(
            &mut store,
            &metadata.config.encryption,
            &master_key,
            self.lock_context,
            &mut self.lock_handler,
        )?;

        // We read the metadata again after acquiring a lock but before getting the header ID to
        // avoid a race condition. We don't have to worry about decrypting the master encryption key
        // again because the master encryption key should never change.
        let serialized_metadata = store
            .read_block(BlockKey::Super)
            .map_err(crate::Error::Store)?
            .ok_or(crate::Error::Corrupt)?;
        let metadata: RepoMetadata =
            from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // Read, decrypt, decompress, and deserialize the repository header.
        let encrypted_header = store
            .read_block(BlockKey::Header(metadata.header_id))
            .map_err(crate::Error::Store)?
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
            instances,
            handle_table,
        } = header;

        let state = Arc::new(RwLock::new(RepoState {
            store: Mutex::new(Box::new(store)),
            metadata,
            chunks,
            packs,
            transactions: LockTable::new(),
            master_key,
            lock_id,
        }));

        let repo: KeyRepo<R::Key> = KeyRepo {
            state,
            instance_id: self.instance,
            objects: HashMap::new(),
            instances,
            handle_table,
            transaction_id: Arc::new(Uuid::new_v4()),
        };

        repo.change_instance(self.instance)
    }

    /// Create a new repository, failing if one already exists.
    fn create_repo<R: OpenRepo>(
        &mut self,
        mut store: impl DataStore + 'static,
    ) -> crate::Result<R> {
        let password = match self.password {
            Some(password) if self.config.encryption != Encryption::None => Some(password),
            // Return an error if a password was required but not provided.
            None if self.config.encryption != Encryption::None => {
                return Err(crate::Error::Password)
            }
            _ => None,
        };

        // Check if the repository already exists.
        if store
            .read_block(BlockKey::Version)
            .map_err(crate::Error::Store)?
            .is_some()
        {
            return Err(crate::Error::AlreadyExists);
        }

        // Generate the master encryption key.
        let master_key = match password {
            Some(..) => EncryptionKey::generate(self.config.encryption.key_size()),
            None => EncryptionKey::new(Vec::new()),
        };

        // Attempt to acquire a lock on the data store.
        let lock_id = lock_store(
            &mut store,
            &self.config.encryption,
            &master_key,
            self.lock_context,
            &mut self.lock_handler,
        )?;

        let salt = match password {
            Some(..) => KeySalt::generate(),
            None => KeySalt::empty(),
        };

        // Encrypt the master encryption key.
        let encrypted_master_key = match password {
            Some(password_bytes) => {
                let user_key = EncryptionKey::derive(
                    password_bytes,
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
        let header = Header {
            chunks: HashMap::new(),
            packs: HashMap::new(),
            instances: HashMap::new(),
            handle_table: HandleIdTable::new(),
        };

        // Serialize, encode, and write the header to the data store.
        let serialized_header =
            to_vec(&header).expect("Could not serialize the repository header.");
        let compressed_header = self.config.compression.compress(&serialized_header)?;
        let encrypted_header = self
            .config
            .encryption
            .encrypt(&compressed_header, &master_key);
        let header_id = Uuid::new_v4().into();
        store
            .write_block(BlockKey::Header(header_id), &encrypted_header)
            .map_err(crate::Error::Store)?;

        // Create the repository metadata with the header block references.
        let metadata = RepoMetadata {
            id: Uuid::new_v4().into(),
            config: self.config.clone(),
            master_key: encrypted_master_key,
            salt,
            header_id,
        };

        // Write the repository metadata.
        let serialized_metadata = to_vec(&metadata).expect("Could not serialize metadata.");
        store
            .write_block(BlockKey::Super, &serialized_metadata)
            .map_err(crate::Error::Store)?;

        // Write the repository version. We do this last because this signifies that the repository
        // is done being created.
        store
            .write_block(BlockKey::Version, VERSION_ID.as_bytes())
            .map_err(crate::Error::Store)?;

        let Header {
            chunks,
            packs,
            instances,
            handle_table,
        } = header;

        let state = Arc::new(RwLock::new(RepoState {
            store: Mutex::new(Box::new(store)),
            metadata,
            chunks,
            packs,
            transactions: LockTable::new(),
            master_key,
            lock_id,
        }));

        let repo: KeyRepo<R::Key> = KeyRepo {
            state,
            instance_id: self.instance,
            objects: HashMap::new(),
            instances,
            handle_table,
            transaction_id: Arc::new(Uuid::new_v4()),
        };

        repo.change_instance(self.instance)
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
    pub fn open<R, C>(&mut self, config: &C) -> crate::Result<R>
    where
        R: OpenRepo,
        C: OpenStore,
    {
        let mut store = config.open()?;

        match self.mode {
            OpenMode::Open => self.open_repo(store),
            OpenMode::Create => {
                if store
                    .read_block(BlockKey::Version)
                    .map_err(crate::Error::Store)?
                    .is_some()
                {
                    self.open_repo(store)
                } else {
                    self.create_repo(store)
                }
            }
            OpenMode::CreateNew => self.create_repo(store),
        }
    }
}

impl<'a> Debug for OpenOptions<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenOptions")
            .field("config", &self.config)
            .field("mode", &self.mode)
            .field("password", &self.password)
            .field("instance", &self.instance)
            .field("lock_context", &self.lock_context)
            .finish_non_exhaustive()
    }
}
