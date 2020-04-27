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

use std::collections::HashSet;
use std::io::{Read, Write};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::repo::content::hash::HashAlgorithm;
use crate::repo::version_id::{check_version, write_version};
use crate::repo::{
    LockStrategy, ObjectRepository, OpenRepo, ReadOnlyObject, RepositoryConfig, RepositoryInfo,
    RepositoryStats,
};
use crate::store::DataStore;

lazy_static! {
    /// The current repository format version ID.
    static ref VERSION_ID: Uuid = Uuid::parse_str("c8903e2a-6092-11ea-b0bb-3bbaa967b54a").unwrap();
}

/// The size of the buffer to use when copying bytes.
const BUFFER_SIZE: usize = 4096;

/// The key to use in the `ObjectRepository` which backs a `ContentRepository`.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum ContentKey {
    /// An object identified by its hash.
    Object(Vec<u8>),

    /// A location to write data to before we know its hash.
    Stage,

    /// The serialized hash algorithm in use by this repository.
    HashAlgorithm,

    /// The serialized repository version ID.
    RepositoryVersion,
}

/// The default hash algorithm to use for `ContentRepository`.
const DEFAULT_ALGORITHM: HashAlgorithm = HashAlgorithm::Blake2b(32);

/// A content-addressable storage.
///
/// This is a wrapper around `ObjectRepository` which allows for accessing data by its cryptographic
/// hash. See `HashAlgorithm` for a list of supported hash algorithms. The default hash algorithm is
/// 256-bit BLAKE2b, but this can be changed using `change_algorithm`.
///
/// Like `ObjectRepository`, changes made to the repository are not persisted to the data store
/// until `commit` is called. For details about deduplication, compression, encryption, and locking,
/// see `ObjectRepository`.
#[derive(Debug)]
pub struct ContentRepository<S: DataStore> {
    repository: ObjectRepository<ContentKey, S>,
    hash_algorithm: HashAlgorithm,
}

impl<S: DataStore> OpenRepo<S> for ContentRepository<S> {
    fn open_repo(store: S, strategy: LockStrategy, password: Option<&[u8]>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let repository = ObjectRepository::open_repo(store, strategy, password)?;

        // Read the repository version.
        let object = repository
            .get(&ContentKey::RepositoryVersion)
            .ok_or(crate::Error::NotFound)?;
        check_version(object, *VERSION_ID)?;

        // Read the hash algorithm.
        let mut object = repository
            .get(&ContentKey::HashAlgorithm)
            .ok_or(crate::Error::Corrupt)?;
        let hash_algorithm = object.deserialize()?;
        drop(object);

        Ok(Self {
            repository,
            hash_algorithm,
        })
    }

    fn new_repo(store: S, config: RepositoryConfig, password: Option<&[u8]>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut repository = ObjectRepository::new_repo(store, config, password)?;

        // Write the repository version.
        let object = repository.insert(ContentKey::RepositoryVersion);
        write_version(object, *VERSION_ID)?;

        // Write the hash algorithm.
        let mut object = repository.insert(ContentKey::HashAlgorithm);
        object.serialize(&DEFAULT_ALGORITHM)?;
        drop(object);

        repository.commit()?;

        Ok(Self {
            repository,
            hash_algorithm: DEFAULT_ALGORITHM,
        })
    }

    fn create_repo(
        mut store: S,
        config: RepositoryConfig,
        strategy: LockStrategy,
        password: Option<&[u8]>,
    ) -> crate::Result<Self>
    where
        Self: Sized,
    {
        if store.list_blocks().map_err(anyhow::Error::from)?.is_empty() {
            Self::new_repo(store, config, password)
        } else {
            Self::open_repo(store, strategy, password)
        }
    }
}

impl<S: DataStore> ContentRepository<S> {
    /// Return whether the repository contains an object with the given `hash`.
    pub fn contains(&self, hash: &[u8]) -> bool {
        self.repository.contains(&ContentKey::Object(hash.to_vec()))
    }

    /// Add the given `data` to the repository as a new object and return its hash.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn put(&mut self, mut data: impl Read) -> crate::Result<Vec<u8>> {
        let mut buffer = [0u8; BUFFER_SIZE];
        let mut digest = self.hash_algorithm.digest();
        let mut bytes_read;

        // We write to a temporary object because we don't know the data's hash yet.
        let mut object = self.repository.insert(ContentKey::Stage);

        // Calculate the hash and write to the repository simultaneously so the `data` is only read
        // once.
        loop {
            bytes_read = data.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            digest.input(&buffer[..bytes_read]);
            object.write_all(&buffer[..bytes_read])?;
        }

        object.flush()?;
        drop(object);

        // Now that we know the hash, we can associate the object with its hash.
        let hash = digest.result();
        let key = ContentKey::Object(hash.clone());
        if !self.repository.contains(&key) {
            self.repository.copy(&ContentKey::Stage, key)?;
        }
        self.repository.remove(&ContentKey::Stage);

        Ok(hash)
    }

    /// Remove the object with the given `hash` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    pub fn remove(&mut self, hash: &[u8]) -> bool {
        self.repository.remove(&ContentKey::Object(hash.to_vec()))
    }

    /// Return the object with the given `hash` or `None` if it doesn't exist.
    pub fn get(&self, hash: &[u8]) -> Option<ReadOnlyObject<ContentKey, S>> {
        self.repository.get(&ContentKey::Object(hash.to_vec()))
    }

    /// Return an iterator of hashes of all the objects in this repository.
    pub fn list(&self) -> impl Iterator<Item = &[u8]> {
        self.repository.keys().filter_map(|key| match key {
            ContentKey::Object(hash) => Some(hash.as_slice()),
            _ => None,
        })
    }

    /// Return the hash algorithm used by this repository.
    pub fn algorithm(&self) -> HashAlgorithm {
        self.hash_algorithm
    }

    /// Change the hash algorithm used by this repository.
    ///
    /// This re-computes the hashes of all the objects in the repository. If the given hash
    /// algorithm is the same as the current hash algorithm, this does nothing.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn change_algorithm(&mut self, new_algorithm: HashAlgorithm) -> crate::Result<()> {
        if new_algorithm == self.hash_algorithm {
            return Ok(());
        }

        self.hash_algorithm = new_algorithm;

        // Serialize and write the new hash algorithm.
        let mut object = self.repository.insert(ContentKey::HashAlgorithm);
        object.serialize(&new_algorithm)?;
        drop(object);

        // Re-compute the hashes of the objects in the repository.
        let old_hashes = self.list().map(|hash| hash.to_vec()).collect::<Vec<_>>();
        for old_hash in old_hashes {
            let mut object = self.get(&old_hash).unwrap();
            let new_hash = new_algorithm.hash(&mut object)?;
            drop(object);
            let old_key = ContentKey::Object(old_hash);
            self.repository
                .copy(&old_key, ContentKey::Object(new_hash))?;
            self.repository.remove(&old_key);
        }

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        self.repository.commit()
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of hashes of objects which are corrupt.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&self) -> crate::Result<HashSet<&[u8]>> {
        Ok(self
            .repository
            .verify()?
            .iter()
            .filter_map(|key| match key {
                ContentKey::Object(hash) => Some(hash.as_slice()),
                _ => None,
            })
            .collect())
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepository::change_password` for details.
    #[cfg(feature = "encryption")]
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repository.change_password(new_password)
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepositoryInfo {
        self.repository.info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// See `ObjectRepository::peek_info` for details.
    pub fn peek_info(store: &mut S) -> crate::Result<RepositoryInfo> {
        ObjectRepository::<ContentKey, S>::peek_info(store)
    }

    /// Calculate statistics about the repository.
    pub fn stats(&self) -> RepositoryStats {
        self.repository.stats()
    }

    /// Consume this repository and return the wrapped `DataStore`.
    pub fn into_store(self) -> S {
        self.repository.into_store()
    }
}
