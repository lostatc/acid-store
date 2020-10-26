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

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::mem;

use hex_literal::hex;
use uuid::Uuid;

use crate::repo::common::check_version;
use crate::repo::content::hash::HashAlgorithm;
use crate::repo::object::{ObjectHandle, ObjectRepo};
use crate::repo::{ConvertRepo, ReadOnlyObject, RepoInfo};
use crate::store::DataStore;

/// The ID of the managed object which stores the table of keys for the repository.
const TABLE_OBJECT_ID: Uuid = Uuid::from_bytes(hex!("c5319b76 bd43 11ea 90d4 971a5898591d"));

/// The ID of the managed object which stores the hash algorithm.
const ALGORITHM_OBJECT_ID: Uuid = Uuid::from_bytes(hex!("0e4d5b00 bd45 11ea 9fe3 b3eccafb5a4b"));

/// The current repository format version ID.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("e94d5a1e bd42 11ea bbec ebbbc536f7fb"));

/// The size of the buffer to use when copying bytes.
const BUFFER_SIZE: usize = 4096;

/// The default hash algorithm to use for `ContentRepo`.
const DEFAULT_ALGORITHM: HashAlgorithm = HashAlgorithm::Blake2b(32);

/// A content-addressable storage.
#[derive(Debug)]
pub struct ContentRepo<S: DataStore> {
    repository: ObjectRepo<S>,
    hash_table: HashMap<Vec<u8>, ObjectHandle>,
    hash_algorithm: HashAlgorithm,
}

impl<S: DataStore> ConvertRepo<S> for ContentRepo<S> {
    fn from_repo(mut repository: ObjectRepo<S>) -> crate::Result<Self> {
        if check_version(&mut repository, VERSION_ID)? {
            // Read and deserialize the table of content hashes.
            let mut object = repository
                .managed_object(TABLE_OBJECT_ID)
                .ok_or(crate::Error::Corrupt)?;
            let hash_table = object.deserialize()?;

            // Read the hash algorithm.
            let mut object = repository
                .managed_object(ALGORITHM_OBJECT_ID)
                .ok_or(crate::Error::Corrupt)?;
            let hash_algorithm = object.deserialize()?;
            drop(object);

            Ok(Self {
                repository,
                hash_table,
                hash_algorithm,
            })
        } else {
            // Create and write the table of content hashes.
            let mut object = repository.add_managed(TABLE_OBJECT_ID);
            let hash_table = HashMap::new();
            object.serialize(&hash_table)?;
            drop(object);

            // Write the hash algorithm.
            let mut object = repository.add_managed(ALGORITHM_OBJECT_ID);
            object.serialize(&DEFAULT_ALGORITHM)?;
            drop(object);

            repository.commit()?;

            Ok(Self {
                repository,
                hash_table,
                hash_algorithm: DEFAULT_ALGORITHM,
            })
        }
    }

    fn into_repo(mut self) -> crate::Result<ObjectRepo<S>> {
        self.commit()?;
        Ok(self.repository)
    }
}

impl<S: DataStore> ContentRepo<S> {
    /// Return whether the repository contains an object with the given `hash`.
    pub fn contains(&self, hash: &[u8]) -> bool {
        self.hash_table.contains_key(hash)
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

        // Create an object to write the data to.
        let mut stage_handle = self.repository.add_unmanaged();
        let mut stage_object = self
            .repository
            .unmanaged_object_mut(&mut stage_handle)
            .unwrap();

        // Calculate the hash and write to the repository simultaneously so the `data` is only read
        // once.
        loop {
            bytes_read = data.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            digest.input(&buffer[..bytes_read]);
            stage_object.write_all(&buffer[..bytes_read])?;
        }

        stage_object.flush()?;
        drop(stage_object);

        // Now that we know the hash, we can associate the object with its hash.
        let hash = digest.result();
        if self.hash_table.contains_key(&hash) {
            // The data is already in the repository. Delete the object.
            self.repository.remove_unmanaged(&stage_handle);
        } else {
            self.hash_table.insert(hash.clone(), stage_handle);
        }

        Ok(hash)
    }

    /// Remove the object with the given `hash` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    pub fn remove(&mut self, hash: &[u8]) -> bool {
        let handle = match self.hash_table.remove(hash) {
            Some(handle) => handle,
            None => return false,
        };
        self.repository.remove_unmanaged(&handle);
        true
    }

    /// Return a `ReadOnlyObject` for reading the data with the given `hash`.
    ///
    /// This returns `None` if there is no data with the given `hash` in the repository.
    pub fn object(&self, hash: &[u8]) -> Option<ReadOnlyObject<S>> {
        let handle = self.hash_table.get(hash)?;
        self.repository.unmanaged_object(handle)
    }

    /// Return an iterator of hashes of all the objects in this repository.
    pub fn list(&self) -> impl Iterator<Item = &[u8]> {
        self.hash_table.keys().map(|hash| hash.as_slice())
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
        let mut object = self.repository.add_managed(ALGORITHM_OBJECT_ID);
        object.serialize(&new_algorithm)?;
        drop(object);

        // Re-compute the hashes of the objects in the repository.
        let old_table = mem::replace(&mut self.hash_table, HashMap::new());
        for (_, object_handle) in old_table {
            let mut object = self.repository.unmanaged_object(&object_handle).unwrap();
            let new_hash = new_algorithm.hash(&mut object)?;
            drop(object);
            self.hash_table.insert(new_hash, object_handle);
        }

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepo::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Serialize and write the table of content hashes.
        let mut object = self
            .repository
            .managed_object_mut(TABLE_OBJECT_ID)
            .expect("Managed object containing table of content hashes not found in repository.");
        object.serialize(&self.hash_table)?;
        drop(object);

        // Commit the underlying repository.
        self.repository.commit()
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of hashes of objects which are corrupt.
    ///
    /// If you just need to verify the integrity of one object, `Object::verify` is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&self) -> crate::Result<HashSet<&[u8]>> {
        let report = self.repository.verify()?;
        Ok(self
            .hash_table
            .iter()
            .filter(|(_, handle)| !report.check_unmanaged(handle))
            .map(|(hash, _)| hash.as_slice())
            .collect::<HashSet<_>>())
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepo::change_password` for details.
    #[cfg(feature = "encryption")]
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repository.change_password(new_password)
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.repository.info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// See `ObjectRepo::peek_info` for details.
    pub fn peek_info(store: &mut S) -> crate::Result<RepoInfo> {
        ObjectRepo::peek_info(store)
    }
}
