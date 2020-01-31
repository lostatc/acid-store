/*
 * Copyright 2019-2020 Garrett Powell
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

use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::RwLock;
use std::time::SystemTime;

use rmp_serde::{from_read, to_vec};
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::store::DataStore;

use super::config::RepositoryConfig;
use super::encryption::{Encryption, EncryptionKey, KeySalt};
use super::header::{Header, Key};
use super::lock::{Lock, LockTable};
use super::metadata::{RepositoryInfo, RepositoryMetadata, RepositoryStats};
use super::object::{chunk_hash, ChunkHash, Object, ObjectHandle};

lazy_static! {
    /// The block ID of the block which stores unencrypted metadata for the repository.
    static ref METADATA_BLOCK_ID: Uuid =
        Uuid::parse_str("8691d360-29c6-11ea-8bc1-2fc8cfe66f33").unwrap();

    /// The block ID of the block which stores the repository format version.
    static ref VERSION_BLOCK_ID: Uuid =
        Uuid::parse_str("cbf28b1c-3550-11ea-8cb0-87d7a14efe10").unwrap();

    /// The current repository format version ID.
    static ref VERSION_ID: Uuid =
        Uuid::parse_str("25bec50c-3551-11ea-8700-3bde18597598").unwrap();

    /// A table of locks on repositories.
    static ref REPO_LOCKS: RwLock<LockTable> = RwLock::new(LockTable::new());
}

/// A strategy for handling a repository which is already locked.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub enum LockStrategy {
    /// Return immediately with an `Err`.
    Abort,

    /// Block and wait for the lock on the repository to be released.
    Wait,
}

/// A persistent object store.
///
/// An `ObjectRepository` maps keys of type `K` to binary blobs called objects and stores them
/// persistently in a `DataStore`.
///
/// Data in a repository is transparently deduplicated using content-defined block-level
/// deduplication via the ZPAQ chunking algorithm. The data and metadata in the repository can
/// optionally be compressed and encrypted.
///
/// A repository cannot be open more than once simultaneously. Once it is opened, it is locked from
/// further open attempts until it is dropped. These locks are only valid for the current user on
/// the local machine and do not protect against multiple users or multiple machines trying to open
/// a repository simultaneously.
///
/// Changes made to a repository are not persisted to disk until `commit` is called.
///
/// # Encryption
/// If encryption is enabled, the Argon2id key derivation function is used to derive a key from a
/// user-supplied password. This key is used to encrypt the repository's randomly generated master
/// key, which is used to encrypt all data in the repository. This setup means that the repository's
/// password can be changed without re-encrypting any data.
///
/// The master key is generated using the operating system's secure random number generator. Both
/// the master key and the derived key are zeroed in memory once they go out of scope.
///
/// Data in a data store is identified by UUIDs and not hashes, so data hashes are not leaked. The
/// repository does not attempt to hide the size of chunks produced by the chunking algorithm, but
/// information about which chunks belong to which objects is encrypted.
///
/// The information in `RepositoryInfo` is not encrypted.
#[derive(Debug)]
pub struct ObjectRepository<K: Key, S: DataStore> {
    /// The data store which backs this repository.
    store: S,

    /// The metadata for the repository.
    metadata: RepositoryMetadata,

    /// The repository's header.
    header: Header<K>,

    /// The master encryption key for the repository.
    master_key: EncryptionKey,

    /// The lock on this repository.
    lock: Lock,
}

impl<K: Key, S: DataStore> ObjectRepository<K, S> {
    /// Create a new repository backed by the given data `store`.
    ///
    /// A `config` must be provided to configure the new repository. If encryption is enabled, a
    /// `password` must be provided; otherwise, this argument can be `None`.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: A repository already exists in the given `store`.
    /// - `Error::Password` A password was required but not provided or provided but not required.
    /// - `Error::Store`: An error occurred with the data store.
    pub fn create_repo(
        mut store: S,
        config: RepositoryConfig,
        password: Option<&[u8]>,
    ) -> crate::Result<Self> {
        // Return an error if a password was required but not provided.
        if password.is_none() && config.encryption != Encryption::None {
            return Err(crate::Error::Password);
        }

        // Return an error if a password was provided but not required.
        if password.is_some() && config.encryption == Encryption::None {
            return Err(crate::Error::Password);
        }

        // Acquire an exclusive lock on the repository.
        let id = Uuid::new_v4();
        let lock = REPO_LOCKS
            .write()
            .unwrap()
            .acquire_lock(id, LockStrategy::Abort)
            .map_err(|_| crate::Error::AlreadyExists)?;

        // Check if the repository already exists.
        if store
            .read_block(*VERSION_BLOCK_ID)
            .map_err(anyhow::Error::from)?
            .is_some()
        {
            return Err(crate::Error::AlreadyExists);
        }

        // Generate the master encryption key.
        let master_key = match password {
            Some(..) => EncryptionKey::generate(config.encryption.key_size()),
            None => EncryptionKey::new(Vec::new()),
        };

        // Encrypt the master encryption key.
        let salt = KeySalt::generate();
        let encrypted_master_key = match password {
            Some(password_bytes) => {
                let user_key = EncryptionKey::derive(
                    password_bytes,
                    &salt,
                    config.encryption.key_size(),
                    config.memory_limit.to_mem_limit(),
                    config.operations_limit.to_ops_limit(),
                );
                config.encryption.encrypt(master_key.as_ref(), &user_key)
            }
            None => Vec::new(),
        };

        // Generate and write the header.
        let header = Header::default();
        let serialized_header = to_vec(&header).expect("Could not serialize header.");
        let compressed_header = config.compression.compress(&serialized_header)?;
        let encrypted_header = config.encryption.encrypt(&compressed_header, &master_key);
        let header_id = Uuid::new_v4();
        store
            .write_block(header_id, &encrypted_header)
            .map_err(anyhow::Error::from)?;

        // Create the repository metadata with a reference to the newly-written header.
        let metadata = RepositoryMetadata {
            id,
            chunker_bits: config.chunker_bits,
            compression: config.compression,
            encryption: config.encryption,
            memory_limit: config.memory_limit,
            operations_limit: config.operations_limit,
            master_key: encrypted_master_key,
            salt,
            header: header_id,
            creation_time: SystemTime::now(),
        };

        // Write the repository metadata.
        let serialized_metadata = to_vec(&metadata).expect("Could not serialize metadata.");
        store
            .write_block(*METADATA_BLOCK_ID, &serialized_metadata)
            .map_err(anyhow::Error::from)?;

        // Write the repository version. We do this last because this signifies that the repository
        // is done being created.
        store
            .write_block(*VERSION_BLOCK_ID, VERSION_ID.as_bytes())
            .map_err(anyhow::Error::from)?;

        Ok(ObjectRepository {
            store,
            metadata,
            header,
            master_key,
            lock,
        })
    }

    /// Open the repository in the given data `store`.
    ///
    /// If encryption is enabled, a `password` must be provided. Otherwise, this argument can be
    /// `None`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no repository in the given `store`.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Locked`: The repository is locked and `LockStrategy::Abort` was used.
    /// - `Error::Password`: The password provided is invalid.
    /// - `Error::KeyType`: The type `K` does not match the data in the repository.
    /// - `Error::UnsupportedFormat`: This repository format is not supported by this version of
    /// the library.
    /// - `Error::Store`: An error occurred with the data store.
    pub fn open_repo(
        mut store: S,
        password: Option<&[u8]>,
        strategy: LockStrategy,
    ) -> crate::Result<Self> {
        // Acquire a lock on the repository.
        let repository_id = Self::peek_info(&mut store)?.id();
        let lock = REPO_LOCKS
            .write()
            .unwrap()
            .acquire_lock(repository_id, strategy)?;

        // Read the repository version to see if this is a compatible repository.
        let serialized_version = store
            .read_block(*VERSION_BLOCK_ID)
            .map_err(anyhow::Error::from)?
            .ok_or(crate::Error::NotFound)?;
        let version =
            Uuid::from_slice(serialized_version.as_slice()).map_err(|_| crate::Error::Corrupt)?;
        if version != *VERSION_ID {
            return Err(crate::Error::UnsupportedFormat);
        }

        // We read the metadata again after reading the UUID to prevent a race condition when
        // acquiring the lock.
        let serialized_metadata = store
            .read_block(*METADATA_BLOCK_ID)
            .map_err(anyhow::Error::from)?
            .ok_or(crate::Error::Corrupt)?;
        let metadata: RepositoryMetadata =
            from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // Decrypt the master key for the repository.
        let master_key = match password {
            Some(password_bytes) => {
                let user_key = EncryptionKey::derive(
                    password_bytes,
                    &metadata.salt,
                    metadata.encryption.key_size(),
                    metadata.memory_limit.to_mem_limit(),
                    metadata.operations_limit.to_ops_limit(),
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

        // Read, decrypt, decompress, and deserialize the header.
        let encrypted_header = store
            .read_block(metadata.header)
            .map_err(anyhow::Error::from)?
            .ok_or(crate::Error::Corrupt)?;
        let compressed_header = metadata
            .encryption
            .decrypt(&encrypted_header, &master_key)
            .map_err(|_| crate::Error::Corrupt)?;
        let serialized_header = metadata
            .compression
            .decompress(&compressed_header)
            .map_err(|_| crate::Error::Corrupt)?;
        let header: Header<K> =
            from_read(serialized_header.as_slice()).map_err(|_| crate::Error::KeyType)?;

        Ok(ObjectRepository {
            store,
            metadata,
            header,
            master_key,
            lock,
        })
    }

    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.header.objects.contains_key(key)
    }

    /// Insert the given `key` into the repository and return a new object.
    ///
    /// If the given `key` already exists in the repository, its object is replaced. The returned
    /// object represents the data associated with the `key`.
    pub fn insert(&mut self, key: K) -> Object<K, S> {
        self.header
            .objects
            .insert(key.clone(), ObjectHandle::default());
        self.header.clean_chunks();
        Object::new(self, key, self.metadata.chunker_bits as usize)
    }

    /// Remove the object associated with `key` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = self.header.objects.remove(key);
        self.header.clean_chunks();
        handle.is_some()
    }

    /// Return the object associated with `key` or `None` if it doesn't exist.
    pub fn get<Q>(&mut self, key: &Q) -> Option<Object<K, S>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ToOwned<Owned = K> + ?Sized,
    {
        self.header.objects.get(&key)?;
        Some(Object::new(
            self,
            key.to_owned(),
            self.metadata.chunker_bits as usize,
        ))
    }

    /// Return an iterator over all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.header.objects.keys()
    }

    /// Copy the object at `source` to `dest`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the object.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no object at `source`.
    /// - `Error::AlreadyExists`: There is already an object at `dest`.
    pub fn copy<Q>(&mut self, source: &Q, dest: K) -> crate::Result<()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if self.contains(dest.borrow()) {
            return Err(crate::Error::AlreadyExists);
        }

        let source_object = self
            .header
            .objects
            .get(source)
            .ok_or(crate::Error::NotFound)?
            .clone();

        self.header.objects.insert(dest, source_object);

        Ok(())
    }

    /// Compress and encrypt the given `data` and return it.
    fn encode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let compressed_data = self.metadata.compression.compress(data)?;

        Ok(self
            .metadata
            .encryption
            .encrypt(compressed_data.as_slice(), &self.master_key))
    }

    /// Decrypt and decompress the given `data` and return it.
    fn decode_data(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        let decrypted_data = self.metadata.encryption.decrypt(data, &self.master_key)?;

        Ok(self
            .metadata
            .compression
            .decompress(decrypted_data.as_slice())?)
    }

    /// Write the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum may be returned without
    /// writing any new data.
    pub(super) fn write_chunk(&mut self, data: &[u8]) -> crate::Result<ChunkHash> {
        // Get a checksum of the unencoded data.
        let checksum = chunk_hash(data);

        // Check if the chunk already exists.
        if self.header.chunks.contains_key(&checksum) {
            return Ok(checksum);
        }

        // Encode the data and write it to the data store.
        let encoded_data = self.encode_data(data)?;
        let block_id = Uuid::new_v4();
        self.store
            .write_block(block_id, &encoded_data)
            .map_err(anyhow::Error::from)?;

        // Add the chunk to the header.
        self.header.chunks.insert(checksum, block_id);

        Ok(checksum)
    }

    /// Return the bytes of the chunk with the given checksum or `None` if there is none.
    pub(super) fn read_chunk(&mut self, checksum: &ChunkHash) -> crate::Result<Vec<u8>> {
        let chunk_id = self.header.chunks[checksum];
        let chunk = self
            .store
            .read_block(chunk_id)
            .map_err(anyhow::Error::from)?
            .ok_or(crate::Error::InvalidData)?;
        self.decode_data(chunk.as_slice())
    }

    /// Get the object handle for the object associated with `key`.
    pub(super) fn get_handle<Q>(&self, key: &Q) -> &ObjectHandle
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.header.objects.get(key).unwrap()
    }

    /// Get the object handle for the object associated with `key`.
    pub(super) fn get_handle_mut<Q>(&mut self, key: &Q) -> &mut ObjectHandle
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.header.objects.get_mut(key).unwrap()
    }

    /// Return a list of blocks in `store` excluding those used to store metadata.
    fn list_data_blocks(&mut self) -> crate::Result<Vec<Uuid>> {
        Ok(self
            .store
            .list_blocks()
            .map_err(anyhow::Error::from)?
            .iter()
            .copied()
            .filter(|id| {
                *id != *METADATA_BLOCK_ID && *id != *VERSION_BLOCK_ID && *id != self.metadata.header
            })
            .collect())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// No changes are saved persistently until this method is called. Committing a repository is an
    /// atomic and consistent operation; changes cannot be partially committed and interrupting a
    /// commit will never leave the repository in an inconsistent state.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Serialize and encode the header.
        let serialized_header = to_vec(&self.header).expect("Could not serialize header.");
        let encoded_header = self.encode_data(&serialized_header)?;

        // Write the new header to the data store.
        let header_id = Uuid::new_v4();
        self.store
            .write_block(header_id, &encoded_header)
            .map_err(anyhow::Error::from)?;
        self.metadata.header = header_id;

        // Write the repository metadata, atomically completing the commit.
        let serialized_metadata = to_vec(&self.metadata).expect("Could not serialize metadata.");
        self.store
            .write_block(*METADATA_BLOCK_ID, &serialized_metadata)
            .map_err(anyhow::Error::from)?;

        // After changes are committed, remove any unused chunks from the data store.
        let referenced_chunks = self.header.chunks.values().copied().collect::<HashSet<_>>();
        for stored_chunk in self.list_data_blocks()? {
            if !referenced_chunks.contains(&stored_chunk) {
                self.store
                    .remove_block(stored_chunk)
                    .map_err(anyhow::Error::from)?;
            }
        }

        Ok(())
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of keys of objects which are corrupt. This is more efficient than
    /// calling `Object::verify` on each object in the repository.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&mut self) -> crate::Result<HashSet<&K>> {
        let mut corrupt_chunks = HashSet::new();
        let expected_checksums = self.header.chunks.keys().copied().collect::<Vec<_>>();

        // Get the set of hashes of chunks which are corrupt.
        for expected_checksum in expected_checksums {
            match self.read_chunk(&expected_checksum) {
                Ok(data) => {
                    let actual_checksum = chunk_hash(&data);
                    if expected_checksum != actual_checksum {
                        corrupt_chunks.insert(expected_checksum);
                    }
                }
                Err(crate::Error::InvalidData) => {
                    // Ciphertext verification failed. No need to check the hash.
                    corrupt_chunks.insert(expected_checksum);
                }
                Err(error) => return Err(error),
            };
        }

        // If there are no corrupt chunks, there are no corrupt objects.
        if corrupt_chunks.is_empty() {
            return Ok(HashSet::new());
        }

        let mut corrupt_objects = HashSet::new();

        for (key, object) in self.header.objects.iter() {
            for chunk in &object.chunks {
                // If any one of the object's chunks is corrupt, the object is corrupt.
                if corrupt_chunks.contains(&chunk.hash) {
                    corrupt_objects.insert(key);
                    break;
                }
            }
        }

        Ok(corrupt_objects)
    }

    /// Change the password for this repository.
    ///
    /// This replaces the existing password with `new_password`. Changing the password does not
    /// require re-encrypting any data. The change does not take effect until `commit` is called.
    /// If encryption is disabled, this method does nothing.
    pub fn change_password(&mut self, new_password: &[u8]) {
        let salt = KeySalt::generate();
        let user_key = EncryptionKey::derive(
            new_password,
            &salt,
            self.metadata.encryption.key_size(),
            self.metadata.memory_limit.to_mem_limit(),
            self.metadata.operations_limit.to_ops_limit(),
        );
        self.metadata.salt = salt;
        self.metadata.master_key = self
            .metadata
            .encryption
            .encrypt(self.master_key.as_ref(), &user_key);
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepositoryInfo {
        self.metadata.to_info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no repository in the given `store`.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Store`: An error occurred with the data store.
    pub fn peek_info(store: &mut S) -> crate::Result<RepositoryInfo> {
        // Read and deserialize the metadata.
        let serialized_metadata = match store
            .read_block(*METADATA_BLOCK_ID)
            .map_err(anyhow::Error::from)?
        {
            Some(data) => data,
            None => return Err(crate::Error::NotFound),
        };
        let metadata: RepositoryMetadata =
            from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        Ok(metadata.to_info())
    }

    /// Calculate statistics about the repository.
    pub fn stats(&self) -> RepositoryStats {
        let chunks = self
            .header
            .objects
            .values()
            .flat_map(|object| &object.chunks)
            .collect::<HashSet<_>>();
        let apparent_size = self.header.objects.values().map(|object| object.size).sum();
        let actual_size = chunks.iter().map(|chunk| chunk.size as u64).sum();

        RepositoryStats {
            apparent_size,
            actual_size,
        }
    }

    /// Consume this repository and return the wrapped `DataStore`.
    pub fn into_store(self) -> S {
        self.store
    }
}
