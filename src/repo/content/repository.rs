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

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::io::{Read, Write};

use hex_literal::hex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::{
    key::KeyRepo,
    state::{ObjectKey, StateRepo},
    Commit, InstanceId, OpenRepo, ReadOnlyObject, RepoInfo, RestoreSavepoint, Savepoint, VersionId,
};

use super::hash::{HashAlgorithm, BUFFER_SIZE, DEFAULT_ALGORITHM};

/// The state for a `ContentRepo`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoState {
    /// A map of content hashes to IDs for the objects which store the contents.
    pub table: HashMap<Vec<u8>, ObjectKey>,

    /// The currently selected hash algorithm.
    pub algorithm: HashAlgorithm,

    /// The ID of the object which is used to store data while calculating its hash.
    pub stage: Option<ObjectKey>,
}

impl Default for RepoState {
    fn default() -> Self {
        Self {
            table: HashMap::new(),
            algorithm: DEFAULT_ALGORITHM,
            stage: None,
        }
    }
}

/// A content-addressable storage.
///
/// See [`crate::repo::content`] for more information.
#[derive(Debug)]
pub struct ContentRepo(StateRepo<RepoState>);

impl OpenRepo for ContentRepo {
    type Key = <StateRepo<RepoState> as OpenRepo>::Key;

    const VERSION_ID: VersionId = VersionId::new(Uuid::from_bytes(hex!(
        "91e098e0 cfe3 11eb 8823 77511adc39c8"
    )));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(ContentRepo(StateRepo::open_repo(repo)?))
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(ContentRepo(StateRepo::create_repo(repo)?))
    }

    fn into_repo(self) -> crate::Result<KeyRepo<Self::Key>> {
        self.0.into_repo()
    }
}

impl ContentRepo {
    /// Return whether the repository contains an object with the given `hash`.
    pub fn contains(&self, hash: &[u8]) -> bool {
        self.0.state().table.contains_key(hash)
    }

    /// Add the given `data` to the repository as a new object and return its hash.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn put(&mut self, mut data: impl Read) -> crate::Result<Vec<u8>> {
        let mut buffer = [0u8; BUFFER_SIZE];
        let mut digest = self.0.state().algorithm.digest();
        let mut bytes_read;

        // Get a temporary object to write the data to until we know its hash. We re-use the same
        // object so we don't have to worry about cleaning it up if the method errs.
        if self.0.state().stage.is_none() {
            self.0.state_mut().stage = Some(self.0.create());
        }
        let stage_object_id = self.0.state().stage.unwrap();
        let mut stage_object = self.0.object(stage_object_id).unwrap();

        // This object may have data in it from a past failed write.
        stage_object.set_len(0)?;

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

        stage_object.commit()?;
        drop(stage_object);

        // Now that we know the hash, we can associate the object with its hash.
        let hash = digest.result();
        if !self.0.state().table.contains_key(&hash) {
            let object_id = self.0.copy(stage_object_id).unwrap();
            self.0.state_mut().table.insert(hash.clone(), object_id);
        }

        Ok(hash)
    }

    /// Remove the object with the given `hash` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`Commit::clean`] is called.
    ///
    /// [`Commit::clean`]: crate::repo::Commit::clean
    pub fn remove(&mut self, hash: &[u8]) -> bool {
        let object_id = match self.0.state_mut().table.remove(hash) {
            Some(object_id) => object_id,
            None => return false,
        };
        assert!(self.0.remove(object_id));
        true
    }

    /// Return a `ReadOnlyObject` for reading the data with the given `hash`.
    ///
    /// This returns `None` if there is no data with the given `hash` in the repository.
    pub fn object(&self, hash: &[u8]) -> Option<ReadOnlyObject> {
        let object_id = *self.0.state().table.get(hash)?;
        Some(self.0.object(object_id).unwrap().try_into().unwrap())
    }

    /// Return an iterator of hashes of all the objects in this repository.
    pub fn list(&self) -> impl Iterator<Item = &[u8]> {
        self.0.state().table.keys().map(|hash| hash.as_slice())
    }

    /// Return the hash algorithm used by this repository.
    pub fn algorithm(&self) -> HashAlgorithm {
        self.0.state().algorithm
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
        if new_algorithm == self.0.state().algorithm {
            return Ok(());
        }

        // Re-compute the hashes of the objects in the repository.
        let mut new_table = HashMap::new();
        for object_id in self.0.state().table.values() {
            let mut object = self.0.object(*object_id).unwrap();
            let new_hash = new_algorithm.hash(&mut object)?;
            drop(object);
            new_table.insert(new_hash, *object_id);
        }

        self.0.state_mut().algorithm = new_algorithm;
        self.0.state_mut().table = new_table;

        Ok(())
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
        let corrupt_keys = self.0.verify()?;
        Ok(self
            .0
            .state()
            .table
            .iter()
            .filter(|(_, object_id)| corrupt_keys.contains(*object_id))
            .map(|(hash, _)| hash.as_slice())
            .collect())
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.0.clear_instance()
    }

    /// Change the password for this repository.
    ///
    /// See [`KeyRepo::change_password`] for details.
    ///
    /// [`KeyRepo::change_password`]: crate::repo::key::KeyRepo::change_password
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.0.change_password(new_password)
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> InstanceId {
        self.0.instance()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.0.info()
    }
}

impl Commit for ContentRepo {
    fn commit(&mut self) -> crate::Result<()> {
        self.0.commit()
    }

    fn rollback(&mut self) -> crate::Result<()> {
        self.0.rollback()
    }

    fn clean(&mut self) -> crate::Result<()> {
        self.0.clean()
    }
}

impl RestoreSavepoint for ContentRepo {
    type Restore = <StateRepo<RepoState> as RestoreSavepoint>::Restore;

    fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.0.savepoint()
    }

    fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Self::Restore> {
        self.0.start_restore(savepoint)
    }

    fn finish_restore(&mut self, restore: Self::Restore) -> bool {
        self.0.finish_restore(restore)
    }
}
