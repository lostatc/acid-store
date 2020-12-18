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
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::object::{ObjectHandle, ObjectRepo};
use crate::repo::state_helpers::{commit, read_state, rollback, write_state};
use crate::repo::{OpenRepo, ReadOnlyObject, RepoInfo};

use super::hash::{HashAlgorithm, BUFFER_SIZE};

/// The default hash algorithm to use for `ContentRepo`.
const DEFAULT_ALGORITHM: HashAlgorithm = HashAlgorithm::Blake3;

/// The state for a `ContentRepo`.
#[derive(Debug, Serialize, Deserialize)]
struct ContentRepoState {
    hash_table: HashMap<Vec<u8>, ObjectHandle>,
    hash_algorithm: HashAlgorithm,
}

/// A content-addressable storage.
///
/// See [`crate::repo::content`] for more information.
#[derive(Debug)]
pub struct ContentRepo {
    repo: ObjectRepo,
    state: ContentRepoState,
}

impl OpenRepo for ContentRepo {
    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("a5994919 e6b7 4d88 9d7c 7d2c55e398b5"));

    fn open_repo(mut repo: ObjectRepo) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let state = read_state(&mut repo)?;
        Ok(Self { repo, state })
    }

    fn create_repo(mut repo: ObjectRepo) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let state = ContentRepoState {
            hash_table: HashMap::new(),
            hash_algorithm: DEFAULT_ALGORITHM,
        };
        write_state(&mut repo, &state)?;
        Ok(Self { repo, state })
    }

    fn into_repo(mut self) -> crate::Result<ObjectRepo> {
        write_state(&mut self.repo, &self.state)?;
        Ok(self.repo)
    }
}

impl ContentRepo {
    /// Return whether the repository contains an object with the given `hash`.
    pub fn contains(&self, hash: &[u8]) -> bool {
        self.state.hash_table.contains_key(hash)
    }

    /// Add the given `data` to the repository as a new object and return its hash.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn put(&mut self, mut data: impl Read) -> crate::Result<Vec<u8>> {
        let mut buffer = [0u8; BUFFER_SIZE];
        let mut digest = self.state.hash_algorithm.digest();
        let mut bytes_read;

        // Create an object to write the data to.
        let mut stage_handle = self.repo.add_unmanaged();
        let mut stage_object = self.repo.unmanaged_object_mut(&mut stage_handle).unwrap();

        // Calculate the hash and write to the repository simultaneously so the `data` is only read
        // once.
        loop {
            bytes_read = data.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            digest.update(&buffer[..bytes_read]);
            stage_object.write_all(&buffer[..bytes_read])?;
        }

        stage_object.flush()?;
        drop(stage_object);

        // Now that we know the hash, we can associate the object with its hash.
        let hash = digest.result();
        if self.state.hash_table.contains_key(&hash) {
            // The data is already in the repository. Delete the object.
            self.repo.remove_unmanaged(&stage_handle);
        } else {
            self.state.hash_table.insert(hash.clone(), stage_handle);
        }

        Ok(hash)
    }

    /// Remove the object with the given `hash` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// [`clean`]: crate::repo::content::ContentRepo::clean
    pub fn remove(&mut self, hash: &[u8]) -> bool {
        let handle = match self.state.hash_table.remove(hash) {
            Some(handle) => handle,
            None => return false,
        };
        self.repo.remove_unmanaged(&handle);
        true
    }

    /// Return a `ReadOnlyObject` for reading the data with the given `hash`.
    ///
    /// This returns `None` if there is no data with the given `hash` in the repository.
    pub fn object(&self, hash: &[u8]) -> Option<ReadOnlyObject> {
        let handle = self.state.hash_table.get(hash)?;
        self.repo.unmanaged_object(handle)
    }

    /// Return an iterator of hashes of all the objects in this repository.
    pub fn list(&self) -> impl Iterator<Item = &[u8]> {
        self.state.hash_table.keys().map(|hash| hash.as_slice())
    }

    /// Return the hash algorithm used by this repository.
    pub fn algorithm(&self) -> HashAlgorithm {
        self.state.hash_algorithm
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
        if new_algorithm == self.state.hash_algorithm {
            return Ok(());
        }

        self.state.hash_algorithm = new_algorithm;

        // Re-compute the hashes of the objects in the repository.
        let old_table = mem::replace(&mut self.state.hash_table, HashMap::new());
        for (_, object_handle) in old_table {
            let mut object = self.repo.unmanaged_object(&object_handle).unwrap();
            let new_hash = new_algorithm.hash(&mut object)?;
            drop(object);
            self.state.hash_table.insert(new_hash, object_handle);
        }

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See [`ObjectRepo::commit`] for details.
    ///
    /// [`ObjectRepo::commit`]: crate::repo::object::ObjectRepo::commit
    pub fn commit(&mut self) -> crate::Result<()> {
        commit(&mut self.repo, &self.state)
    }

    /// Roll back all changes made since the last commit.
    ///
    /// See [`ObjectRepo::rollback`] for details.
    ///
    /// [`ObjectRepo::rollback`]: crate::repo::object::ObjectRepo::rollback
    pub fn rollback(&mut self) -> crate::Result<()> {
        self.state = rollback(&mut self.repo)?;
        Ok(())
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// See [`ObjectRepo::clean`] for details.
    ///
    /// [`ObjectRepo::clean`]: crate::repo::object::ObjectRepo::clean
    pub fn clean(&mut self) -> crate::Result<()> {
        self.repo.clean()
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        for handle in self.state.hash_table.values() {
            self.repo.remove_unmanaged(handle);
        }
        self.state.hash_table.clear();
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of hashes of objects which are corrupt.
    ///
    /// If you just need to verify the integrity of one object, [`Object::verify`] is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`Object::verify`]: crate::repo::Object::verify
    pub fn verify(&self) -> crate::Result<HashSet<&[u8]>> {
        let report = self.repo.verify()?;
        Ok(self
            .state
            .hash_table
            .iter()
            .filter(|(_, handle)| !report.check_unmanaged(handle))
            .map(|(hash, _)| hash.as_slice())
            .collect::<HashSet<_>>())
    }

    /// Change the password for this repository.
    ///
    /// See [`ObjectRepo::change_password`] for details.
    ///
    /// [`ObjectRepo::change_password`]: crate::repo::object::ObjectRepo::change_password
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repo.change_password(new_password)
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> Uuid {
        self.repo.instance()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.repo.info()
    }
}
