/*
 * Copyright 2019 Garrett Powell
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

use std::collections::{HashMap, HashSet};
use std::fs::{create_dir_all, File};
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io::{self, Read};
use std::path::PathBuf;
use std::sync::RwLock;

use cdchunking::Chunker;
use cdchunking::ZPAQ;
use dirs::{data_dir, runtime_dir};
use fs2::FileExt;
use iter_read::IterRead;
use lazy_static::lazy_static;
use rmp_serde::{from_read, to_vec};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sodiumoxide::crypto::pwhash::argon2id13::{MemLimit, OpsLimit};
use uuid::Uuid;

use crate::store::DataStore;

use super::config::RepositoryConfig;
use super::encryption::{Encryption, EncryptionKey, KeySalt};
use super::hashing::Checksum;
use super::header::{Header, Key};
use super::metadata::RepositoryMetadata;
use super::object::{chunk_hash, ChunkHash, Object};

lazy_static! {
    /// The block ID of the block which stores unencrypted metadata for the repository.
    static ref METADATA_BLOCK_ID: Uuid =
        Uuid::parse_str("8691d360-29c6-11ea-8bc1-2fc8cfe66f33").unwrap();

    /// The path of the directory where repository lock files are stored.
    static ref LOCKS_DIR: PathBuf = runtime_dir()
        .unwrap_or(data_dir().expect("Unsupported platform"))
        .join("data-store")
        .join("locks");

    /// The set of UUIDs of repositories which are currently open.
    static ref OPEN_REPOSITORIES: RwLock<HashSet<Uuid>> = RwLock::new(HashSet::new());
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
/// To store data in the repository, use `write` or `serialize`. To read data from the repository,
/// `get` the `Object` associated with a key and use `read`, `read_all`, or `deserialize` to read
/// the data it represents.
///
/// Data in a repository is transparently deduplicated using content-defined block-level
/// deduplication via the ZPAQ chunking algorithm. The data and metadata in the repository can
/// optionally be compressed and encrypted.
///
/// A repository cannot be open more than once at a time. Once it is opened, it is locked from
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
/// The following information is not encrypted:
/// - The repository's UUID
/// - The configuration values provided via `RepositoryConfig`
/// - The salt used to derive the encryption key
/// - The UUID of the block which stores encrypted metadata
pub struct ObjectRepository<K: Key, S: DataStore> {
    /// The data store which backs this repository.
    store: S,

    /// The metadata for the repository.
    metadata: RepositoryMetadata,

    /// The repository's header.
    header: Header<K>,

    /// The master encryption key for the repository.
    master_key: EncryptionKey,

    /// The lock file for this repository.
    lock_file: File
}

impl<K: Key, S: DataStore> ObjectRepository<K, S> {
    /// Create a new repository backed by the given data `store`.
    ///
    /// A `config` must be provided to configure the new repository. If encryption is enabled, a
    /// `password` must be provided; otherwise, this argument can be `None`.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidInput`: A key was required but not provided or provided but not
    /// required.
    /// - `ErrorKind::WouldBlock`: The repository is locked and `LockStrategy::Abort` was used.
    pub fn create(
        mut store: S,
        config: RepositoryConfig,
        password: Option<&[u8]>,
        strategy: LockStrategy,
    ) -> io::Result<Self> {
        // Return an error if a key was required but not provided.
        if password.is_none() && config.encryption != Encryption::None {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "A key was required but not provided",
            ));
        }

        // Return an error is a key was provided but not required.
        if password.is_some() && config.encryption == Encryption::None {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "A key was provided but not required",
            ));
        }

        // Acquire an exclusive lock on the repository.
        let id = Uuid::new_v4();
        let lock_file = Self::acquire_lock(id, strategy)?;

        // Generate and encrypt the master encryption key.
        let salt = KeySalt::generate();
        let user_key = EncryptionKey::derive(
            password.unwrap_or(&[]),
            &salt,
            config.encryption.key_size(),
            config.memory_limit.to_mem_limit(),
            config.operations_limit.to_ops_limit(),
        );
        let master_key = EncryptionKey::generate(config.encryption.key_size());
        let encrypted_master_key = config.encryption.encrypt(master_key.as_ref(), &user_key);

        // Generate and write the header.
        let header = Header::default();
        let serialized_header = to_vec(&header).expect("Could not serialize header.");
        let compressed_header = config.compression.compress(&serialized_header)?;
        let encrypted_header = config.encryption.encrypt(&compressed_header, &master_key);
        let header_id = Uuid::new_v4();
        store.write_block(&header_id, &encrypted_header)?;

        // Create the repository metadata with a reference to the newly-written header.
        let metadata = RepositoryMetadata {
            id,
            chunker_bits: config.chunker_bits,
            compression: config.compression,
            encryption: config.encryption,
            memory_limit: config.memory_limit.to_mem_limit().0,
            operations_limit: config.operations_limit.to_ops_limit().0,
            hash_algorithm: config.hash_algorithm,
            master_key: encrypted_master_key,
            salt,
            header: header_id,
        };

        // Write the repository metadata.
        let serialized_metadata = to_vec(&metadata).expect("Could not serialize metadata.");
        store.write_block(&METADATA_BLOCK_ID, &serialized_metadata)?;

        Ok(ObjectRepository {
            store,
            metadata,
            header,
            master_key,
            lock_file,
        })
    }

    /// Open the repository in the given data `store`.
    ///
    /// If encryption is enabled, a `password` must be provided. Otherwise, this argument can be
    /// `None`.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidData`: The wrong encryption key was provided.
    /// - `ErrorKind::InvalidData`: The repository is corrupt.
    /// - `ErrorKind::InvalidData`: The type `K` does not match the data in the repository.
    /// - `ErrorKind::WouldBlock`: The repository is locked and `LockStrategy::Abort` was used.
    pub fn open(store: S, password: Option<&[u8]>, strategy: LockStrategy) -> io::Result<Self> {
        // Read and deserialize the metadata.
        let serialized_metadata = store.read_block(&METADATA_BLOCK_ID)?;
        let metadata: RepositoryMetadata = from_read(serialized_metadata.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        let lock_file = Self::acquire_lock(metadata.id, strategy)?;

        // Decrypt the master key for the repository.
        let user_key = EncryptionKey::derive(
            password.unwrap_or(&[]),
            &metadata.salt,
            metadata.encryption.key_size(),
            MemLimit(metadata.memory_limit),
            OpsLimit(metadata.operations_limit),
        );
        let master_key = EncryptionKey::new(
            metadata
                .encryption
                .decrypt(&metadata.master_key, &user_key)?,
        );

        // Read, decrypt, decompress, and deserialize the header.
        let encrypted_header = store.read_block(&metadata.header)?;
        let compressed_header = metadata
            .encryption
            .decrypt(&encrypted_header, &master_key)?;
        let serialized_header = metadata.compression.decompress(&compressed_header)?;
        let header: Header<K> = from_read(serialized_header.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        Ok(ObjectRepository {
            store,
            metadata,
            header,
            master_key,
            lock_file,
        })
    }

    /// Acquire a lock on this repository and return the lock file.
    fn acquire_lock(id: Uuid, strategy: LockStrategy) -> io::Result<File> {
        create_dir_all(LOCKS_DIR.as_path())?;
        let mut buffer = Uuid::encode_buffer();
        let file_name = format!("{}.lock", id.to_hyphenated().encode_lower(&mut buffer));
        let lock_file = OpenOptions::new().write(true).create(true).open(LOCKS_DIR.join(file_name))?;

        // File locks are held on behalf of the entire process, so we need another method of
        // checking if this repository is already open within this process.
        let mut open_repositories = OPEN_REPOSITORIES.write().unwrap();

        if open_repositories.contains(&id) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "This repository is already open.",
            ));
        } else {
            match strategy {
                LockStrategy::Abort => lock_file.try_lock_exclusive()?,
                LockStrategy::Wait => lock_file.lock_exclusive()?
            };

            open_repositories.insert(id);
        }

        Ok(lock_file)
    }

    /// Removes and returns the object with the given `key` from the repository.
    ///
    /// This returns `None` if there is no object with the given `key`.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    pub fn remove(&mut self, key: &K) -> Option<Object> {
        let result = self.header.objects.remove(key);
        self.header.clean_chunks();
        result
    }

    /// Returns a reference to the object with the given `key`, or `None` if it doesn't exist.
    pub fn get(&self, key: &K) -> Option<&Object> {
        self.header.objects.get(key)
    }

    /// Returns an iterator over all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.header.objects.keys()
    }

    /// Returns an iterator over all the keys and objects in this repository.
    pub fn objects(&self) -> impl Iterator<Item = (&K, &Object)> {
        self.header.objects.iter()
    }

    /// Compresses and encrypts the given `data` and returns it.
    fn encode_data(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let compressed_data = self.metadata.compression.compress(data)?;

        Ok(self
            .metadata
            .encryption
            .encrypt(compressed_data.as_slice(), &self.master_key))
    }

    /// Decrypts and decompresses the given `data` and returns it.
    fn decode_data(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let decrypted_data = self.metadata.encryption.decrypt(data, &self.master_key)?;

        Ok(self
            .metadata
            .compression
            .decompress(decrypted_data.as_slice())?)
    }

    /// Writes the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum is returned and no new data is
    /// written.
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<ChunkHash> {
        // Get a checksum of the unencoded data.
        let checksum = chunk_hash(data);

        // Check if the chunk already exists.
        if self.header.chunks.contains_key(&checksum) {
            return Ok(checksum);
        }

        // Encode the data and write it to the data store.
        let encoded_data = self.encode_data(data)?;
        let block_id = Uuid::new_v4();
        self.store.write_block(&block_id, &encoded_data)?;

        // Add the chunk to the header.
        self.header.chunks.insert(checksum, block_id);

        Ok(checksum)
    }

    /// Returns the bytes of the chunk with the given checksum or `None` if there is none.
    fn read_chunk(&self, checksum: &ChunkHash) -> io::Result<Vec<u8>> {
        let chunk_id = &self.header.chunks[checksum];
        self.decode_data(self.store.read_block(chunk_id)?.as_slice())
    }

    /// Writes the given `data` to the repository for a given `key` and returns the object.
    ///
    /// If the given `key` is already in the repository, its data is replaced.
    pub fn write(&mut self, key: K, data: impl Read) -> io::Result<&Object> {
        let chunker = Chunker::new(ZPAQ::new(self.metadata.chunker_bits as usize));

        let mut chunk_hashes = Vec::new();
        let mut digest = self.metadata.hash_algorithm.digest();
        let mut size = 0u64;

        // Split the data into content-defined chunks and write those chunks to the data store.
        for chunk_result in chunker.whole_chunks(data) {
            let chunk = chunk_result?;
            digest.input(&chunk);
            size += chunk.len() as u64;
            chunk_hashes.push(self.write_chunk(&chunk)?);
        }

        let object = Object {
            size,
            checksum: Checksum {
                algorithm: self.metadata.hash_algorithm,
                digest: digest.result_reset().to_vec(),
            },
            chunks: chunk_hashes,
        };

        self.header.objects.remove(&key);
        Ok(self.header.objects.entry(key).or_insert(object))
    }

    /// Serialize the given `value` to the repository for a given `key` and return the object.
    ///
    /// If the given `key` is already in the repository, its data is replaced.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidInput`: The given `value` could not be serialized.
    pub fn serialize(&mut self, key: K, value: &impl Serialize) -> io::Result<&Object> {
        let serialized_value =
            to_vec(&value).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        self.write(key, serialized_value.as_slice())
    }

    /// Returns a reader for reading the data associated with `object` from the repository.
    pub fn read<'a>(&'a self, object: &'a Object) -> impl Read + 'a {
        let chunks = object
            .chunks
            .iter()
            .map(move |checksum| self.read_chunk(checksum));

        IterRead::new(chunks)
    }

    /// Returns a buffer containing the data associated with `object`.
    pub fn read_all(&self, object: &Object) -> io::Result<Vec<u8>> {
        let mut data = Vec::with_capacity(object.size() as usize);
        self.read(object).read_to_end(&mut data)?;
        Ok(data)
    }

    /// Deserializes and returns the data associated with `object`.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidData`: The data could not be deserialized as a value of type `T`.
    pub fn deserialize<T: DeserializeOwned>(&self, object: &Object) -> io::Result<T> {
        let serialized_value = self.read_all(object)?;
        from_read(serialized_value.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Commit changes which have been made to the repository.
    ///
    /// No changes are saved persistently until this method is called. Committing a repository is an
    /// atomic and consistent operation; changes cannot be partially committed and interrupting a
    /// commit will never leave the repository in an inconsistent state.
    pub fn commit(&mut self) -> io::Result<()> {
        // Serialize and encode the header.
        let serialized_header = to_vec(&self.header).expect("Could not serialize header.");
        let encoded_header = self.encode_data(&serialized_header)?;

        // Write the new header to the data store.
        let header_id = Uuid::new_v4();
        self.store.write_block(&header_id, &encoded_header)?;
        self.metadata.header = header_id;

        // Write the repository metadata, atomically completing the commit.
        let serialized_metadata = to_vec(&self.metadata).expect("Could not serialize metadata.");
        self.store
            .write_block(&METADATA_BLOCK_ID, &serialized_metadata)?;

        // After changes are committed, remove any unused chunks from the data store.
        let referenced_chunks = self.header.chunks.values().collect::<HashSet<_>>();
        for stored_chunk in self.store.list_blocks()? {
            if !referenced_chunks.contains(&stored_chunk) {
                self.store.remove_block(&stored_chunk)?;
            }
        }

        Ok(())
    }

    /// Verify the integrity of the data associated with `object`.
    ///
    /// This returns `true` if the object is valid and `false` if it is corrupt.
    pub fn verify_object(&self, object: &Object) -> io::Result<bool> {
        for expected_checksum in &object.chunks {
            let data = self.read_chunk(expected_checksum)?;
            let actual_checksum = chunk_hash(&data);
            if *expected_checksum != actual_checksum {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of keys of objects which are corrupt.
    pub fn verify_repository(&self) -> io::Result<HashSet<&K>> {
        let mut corrupt_objects = HashSet::new();

        // Get a map of all the chunks in the repository to the set of objects they belong to.
        let mut chunks_to_objects = HashMap::new();
        for (key, object) in self.header.objects.iter() {
            for chunk in &object.chunks {
                chunks_to_objects
                    .entry(chunk)
                    .or_insert(HashSet::new())
                    .insert(key);
            }
        }

        for expected_checksum in self.header.chunks.keys() {
            let data = self.read_chunk(expected_checksum)?;
            let actual_checksum = chunk_hash(&data);

            if *expected_checksum != actual_checksum {
                corrupt_objects.extend(chunks_to_objects.remove(expected_checksum).unwrap());
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
            MemLimit(self.metadata.memory_limit),
            OpsLimit(self.metadata.operations_limit),
        );
        self.metadata.salt = salt;
        self.metadata.master_key = self
            .metadata
            .encryption
            .encrypt(self.master_key.as_ref(), &user_key);
    }

    /// Return the UUID of the repository.
    pub fn uuid(&self) -> Uuid {
        self.metadata.id
    }

    /// Return the UUID of the repository at `store` without opening it.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidData`: The repository is corrupt.
    /// - `ErrorKind::InvalidData`: The type `K` does not match the data in the repository.
    pub fn peek_uuid(store: S) -> io::Result<Uuid> {
        // Read and deserialize the metadata.
        let serialized_metadata = store.read_block(&METADATA_BLOCK_ID)?;
        let metadata: RepositoryMetadata = from_read(serialized_metadata.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        Ok(metadata.id)
    }
}

impl<K: Key, S: DataStore> Drop for ObjectRepository<K, S> {
    fn drop(&mut self) {
        // Remove this repository from the set of open stores.
        OPEN_REPOSITORIES.write().unwrap().remove(&self.metadata.id);
    }
}
