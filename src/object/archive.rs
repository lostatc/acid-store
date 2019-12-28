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

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::io::{self, Read};

use cdchunking::Chunker;
use cdchunking::ZPAQ;
use iter_read::IterRead;
use rmp_serde::{from_read, to_vec};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::store::DataStore;

use super::config::RepositoryConfig;
use super::encryption::{Encryption, Key, KeySalt};
use super::hashing::Checksum;
use super::header::Header;
use super::metadata::RepositoryMetadata;
use super::object::{chunk_hash, ChunkHash, Object};

/// A persistent object store.
///
/// An `ObjectArchive` is a binary file format for efficiently storing binary data. An object
/// archive maps keys of type `K` to binary blobs called objects.
///
/// To store data in the archive, use `write` or `serialize`. To read data from the archive, `get`
/// the `Object` associated with a key and use `read`, `read_all`, or `deserialize` to read the data
/// it represents.
///
/// Data in an object archive is transparently deduplicated using content-defined block-level
/// deduplication. The data and metadata in the archive can optionally be compressed and encrypted.
///
/// Object archives use file locking to prevent multiple processes from opening the same archive at
/// the same time. This does not protect against the same archive being opened multiple times
/// within the same process, however. Doing so breaks ACID guarantees.
///
/// Changes made to an `ObjectArchive` are not persisted to disk until `commit` is called.
pub struct ObjectRepository<K, S>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    S: DataStore,
{
    /// The data store which backs this repository.
    store: S,

    /// The metadata for the repository.
    metadata: RepositoryMetadata<S::ChunkId>,

    /// The header as of the last time changes were committed.
    old_header: Header<K, S::ChunkId>,

    /// The current header of the archive.
    header: Header<K, S::ChunkId>,

    /// The master encryption key for the repository.
    master_key: Key,
}

impl<K, S> ObjectRepository<K, S>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
    S: DataStore,
{
    /// Create a new repository backed by the given data `store`.
    ///
    /// A `config` must be provided to configure the new repository. If encryption is enabled, a
    /// `password` must be provided. Otherwise, this argument can be `None`.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidInput`: A key was required but not provided or provided but not
    /// required.
    pub fn create(
        mut store: S,
        config: RepositoryConfig,
        password: Option<&[u8]>,
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

        // Generate and encrypt the master encryption key.
        let salt = KeySalt::generate();
        let user_key = Key::derive(password.unwrap_or(&[]), &salt, config.encryption.key_size());
        let master_key = Key::generate(config.encryption.key_size());
        let encrypted_master_key = config.encryption.encrypt(master_key.as_ref(), &user_key);

        // Generate and write the header.
        let header = Header::default();
        let serialized_header = to_vec(&header).expect("Could not serialize header.");
        let compressed_header = config.compression.compress(&serialized_header)?;
        let encrypted_header = config.encryption.encrypt(&compressed_header, &master_key);
        let header_id = store.write_chunk(&encrypted_header)?;

        // Create the repository metadata with a reference to the newly-written header.
        let metadata = RepositoryMetadata {
            id: Uuid::new_v4(),
            chunker_bits: config.chunker_bits,
            compression: config.compression,
            encryption: config.encryption,
            hash_algorithm: config.hash_algorithm,
            master_key: encrypted_master_key,
            salt,
            header: header_id,
        };

        // Write the repository metadata.
        let serialized_metadata = to_vec(&metadata).expect("Could not serialize metadata.");
        store.write_metadata(&serialized_metadata)?;

        Ok(ObjectRepository {
            store,
            metadata,
            old_header: header.clone(),
            header,
            master_key,
        })
    }

    /// Opens the repository with the given data `store`.
    ///
    /// If encryption is enabled, a `password` must be provided. Otherwise, this argument can be
    /// `None`.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidData`: The repository is corrupt.
    /// - `ErrorKind::InvalidData`: The wrong encryption key was provided.
    pub fn open(store: S, password: Option<&[u8]>) -> io::Result<Self> {
        // Read and deserialize the metadata.
        let serialized_metadata = store.read_metadata()?;
        let metadata: RepositoryMetadata<S::ChunkId> = from_read(serialized_metadata.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        // Decrypt the master key for the repository.
        let user_key = Key::derive(
            password.unwrap_or(&[]),
            &metadata.salt,
            metadata.encryption.key_size(),
        );
        let master_key = Key::new(
            metadata
                .encryption
                .decrypt(&metadata.master_key, &user_key)?,
        );

        // Read, decrypt, decompress, and deserialize the header.
        let encrypted_header = store.read_chunk(&metadata.header)?;
        let compressed_header = metadata
            .encryption
            .decrypt(&encrypted_header, &master_key)?;
        let serialized_header = metadata.compression.decompress(&compressed_header)?;
        let header: Header<K, S::ChunkId> = from_read(serialized_header.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        Ok(ObjectRepository {
            store,
            metadata,
            old_header: header.clone(),
            header,
            master_key,
        })
    }

    /// Removes and returns the object with the given `key` from the archive.
    ///
    /// This returns `None` if there is no object with the given `key`.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called. The size of the archive file will not shrink unless `repack` is called.
    pub fn remove(&mut self, key: &K) -> Option<Object> {
        let result = self.header.objects.remove(key);
        self.header.clean_chunks();
        result
    }

    /// Returns a reference to the object with the given `key`, or `None` if it doesn't exist.
    pub fn get(&self, key: &K) -> Option<&Object> {
        self.header.objects.get(key)
    }

    /// Returns an iterator over all the keys in this archive.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.header.objects.keys()
    }

    /// Returns an iterator over all the keys and objects in this archive.
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
        let decrypted_data = self
            .metadata
            .encryption
            .decrypt(data, &self.master_key)?;

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
        let chunk_id = self.store.write_chunk(&encoded_data)?;

        // Add the chunk to the header.
        self.header.chunks.insert(checksum, chunk_id);

        Ok(checksum)
    }

    /// Returns the bytes of the chunk with the given checksum or `None` if there is none.
    fn read_chunk(&self, checksum: &ChunkHash) -> io::Result<Vec<u8>> {
        let chunk_id = &self.header.chunks[checksum];
        self.decode_data(self.store.read_chunk(chunk_id)?.as_slice())
    }

    /// Writes the given `data` to the archive for a given `key` and returns the object.
    ///
    /// If the given `key` is already in the archive, its data is replaced.
    pub fn write(&mut self, key: K, data: impl Read) -> io::Result<&Object> {
        let chunker = Chunker::new(ZPAQ::new(self.metadata.chunker_bits as usize));

        let mut chunk_hashes = Vec::new();
        let mut digest = self.metadata.hash_algorithm.digest();
        let mut size = 0u64;

        // Split the data into content-defined chunks and write those chunks to the archive.
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

    /// Serializes the given `value` to the archive for a given `key` and returns the object.
    ///
    /// If the given `key` is already in the archive, its data is replaced.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidInput`: The given `value` could not be serialized.
    pub fn serialize(&mut self, key: K, value: &impl Serialize) -> io::Result<&Object> {
        let serialized_value =
            to_vec(&value).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        self.write(key, serialized_value.as_slice())
    }

    /// Returns a reader for reading the data associated with `object` from the archive.
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

    /// Commit changes which have been made to the archive.
    ///
    /// No changes are saved persistently until this method is called. Committing an archive is an
    /// atomic and consistent operation; changes cannot be partially committed and interrupting a
    /// commit will never leave the archive in an inconsistent state.
    pub fn commit(&mut self) -> io::Result<()> {
        // Serialize and encode the header.
        let serialized_header = to_vec(&self.header).expect("Could not serialize header.");
        let encoded_header = self.encode_data(&serialized_header)?;

        // Write the new header to the data store.
        let header_id = self.store.write_chunk(&encoded_header)?;
        self.metadata.header = header_id;

        // Write the repository metadata, atomically completing the commit.
        let serialized_metadata = to_vec(&self.metadata).expect("Could not serialize metadata.");
        self.store.write_metadata(&serialized_metadata)?;

        // Now that changes have been committed, the `old_header` field should be updated.
        self.old_header = self.header.clone();

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

    /// Verify the integrity of all the data in the archive.
    ///
    /// This returns the set of keys of objects which are corrupt.
    pub fn verify_archive(&self) -> io::Result<HashSet<&K>> {
        let mut corrupt_objects = HashSet::new();

        // Get a map of all the chunks in the archive to the set of objects they belong to.
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

    /// Return the UUID of the archive.
    ///
    /// Every archive has a UUID associated with it.
    pub fn uuid(&self) -> Uuid {
        self.metadata.id
    }

    /// Return the UUID of the archive at `path` without opening it.
    ///
    /// Every archive has a UUID associated with it. Reading the UUID does not require decrypting
    /// the archive.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidData`: The repository is corrupt.
    pub fn peek_uuid(store: S) -> io::Result<Uuid> {
        // Read and deserialize the metadata.
        let serialized_metadata = store.read_metadata()?;
        let metadata: RepositoryMetadata<S::ChunkId> = from_read(serialized_metadata.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

        Ok(metadata.id)
    }
}
