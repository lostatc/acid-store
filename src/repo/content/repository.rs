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

use hex_literal::hex;
use uuid::Uuid;

use crate::repo::state_repo::StateKeys;
use crate::repo::{
    state_repo::{OpenStateRepo, StateRepo},
    ReadOnlyObject, RepoInfo, Savepoint,
};

use super::hash::{HashAlgorithm, BUFFER_SIZE};
use super::state::{ContentRepoKey, ContentRepoState, Restore, STATE_KEYS};

/// A content-addressable storage.
///
/// See [`crate::repo::content`] for more information.
#[derive(Debug)]
pub struct ContentRepo(StateRepo<ContentRepoKey, ContentRepoState>);

impl OpenStateRepo for ContentRepo {
    type Key = ContentRepoKey;
    type State = ContentRepoState;
    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("f45f7aa2 be47 11eb aff1 634ee34a5453"));
    const STATE_KEYS: StateKeys<Self::Key> = STATE_KEYS;

    fn from_repo(repo: StateRepo<Self::Key, Self::State>) -> Self {
        ContentRepo(repo)
    }

    fn into_repo(self) -> StateRepo<Self::Key, Self::State> {
        self.0
    }
}

impl ContentRepo {
    /// Return whether the repository contains an object with the given `hash`.
    pub fn contains(&self, hash: &[u8]) -> bool {
        self.0.state.hash_table.contains_key(hash)
    }

    /// Add the given `data` to the repository as a new object and return its hash.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn put(&mut self, mut data: impl Read) -> crate::Result<Vec<u8>> {
        let mut buffer = [0u8; BUFFER_SIZE];
        let mut digest = self.0.state.hash_algorithm.digest();
        let mut bytes_read;

        // Create an object to write the data to.
        let mut stage_object = self.0.repo.insert(ContentRepoKey::Stage);

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
        if !self.0.state.hash_table.contains_key(&hash) {
            let object_id = self.0.state.id_table.next();
            self.0.state.hash_table.insert(hash.clone(), object_id);
            self.0
                .repo
                .copy(&ContentRepoKey::Stage, ContentRepoKey::Object(object_id));
        }

        // Remove the temporary object.
        self.0.repo.remove(&ContentRepoKey::Stage);

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
        let object_id = match self.0.state.hash_table.remove(hash) {
            Some(object_id) => object_id,
            None => return false,
        };
        self.0.state.id_table.recycle(object_id);
        self.0.repo.remove(&ContentRepoKey::Object(object_id));
        true
    }

    /// Return a `ReadOnlyObject` for reading the data with the given `hash`.
    ///
    /// This returns `None` if there is no data with the given `hash` in the repository.
    pub fn object(&self, hash: &[u8]) -> Option<ReadOnlyObject> {
        let object_id = self.0.state.hash_table.get(hash)?;
        self.0.repo.object(&ContentRepoKey::Object(*object_id))
    }

    /// Return an iterator of hashes of all the objects in this repository.
    pub fn list(&self) -> impl Iterator<Item = &[u8]> {
        self.0.state.hash_table.keys().map(|hash| hash.as_slice())
    }

    /// Return the hash algorithm used by this repository.
    pub fn algorithm(&self) -> HashAlgorithm {
        self.0.state.hash_algorithm
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
        if new_algorithm == self.0.state.hash_algorithm {
            return Ok(());
        }

        // Re-compute the hashes of the objects in the repository.
        let mut new_table = HashMap::new();
        for (_, object_id) in &self.0.state.hash_table {
            let mut object = self
                .0
                .repo
                .object(&ContentRepoKey::Object(*object_id))
                .unwrap();
            let new_hash = new_algorithm.hash(&mut object)?;
            drop(object);
            new_table.insert(new_hash, *object_id);
        }

        self.0.state.hash_algorithm = new_algorithm;
        self.0.state.hash_table = new_table;

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See [`KeyRepo::commit`] for details.
    ///
    /// [`KeyRepo::commit`]: crate::repo::key::KeyRepo::commit
    pub fn commit(&mut self) -> crate::Result<()> {
        self.0.commit()
    }

    /// Roll back all changes made since the last commit.
    ///
    /// See [`KeyRepo::rollback`] for details.
    ///
    /// [`KeyRepo::rollback`]: crate::repo::key::KeyRepo::rollback
    pub fn rollback(&mut self) -> crate::Result<()> {
        self.0.rollback()
    }

    /// Create a new `Savepoint` representing the current state of the repository.
    ///
    /// See [`KeyRepo::savepoint`] for details.
    ///
    /// [`KeyRepo::savepoint`]: crate::repo::key::KeyRepo::savepoint
    pub fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.0.savepoint()
    }

    /// Start the process of restoring the repository to the given `savepoint`.
    ///
    /// See [`KeyRepo::start_restore`] for details.
    ///
    /// [`KeyRepo::start_restore`]: crate::repo::key::KeyRepo::start_restore
    pub fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Restore> {
        Ok(Restore(self.0.start_restore(savepoint)?))
    }

    /// Finish the process of restoring the repository to a [`Savepoint`].
    ///
    /// See [`KeyRepo::finish_restore`] for details.
    ///
    /// [`Savepoint`]: crate::repo::Savepoint
    /// [`KeyRepo::finish_restore`]: crate::repo::key::KeyRepo::finish_restore
    pub fn finish_restore(&mut self, restore: Restore) -> bool {
        self.0.finish_restore(restore.0)
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// See [`KeyRepo::clean`] for details.
    ///
    /// [`KeyRepo::clean`]: crate::repo::key::KeyRepo::clean
    pub fn clean(&mut self) -> crate::Result<()> {
        self.0.repo.clean()
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.0.clear_instance()
    }

    /// Delete all data in all instances of the repository.
    ///
    /// See [`KeyRepo::clear_repo`] for details.
    ///
    /// [`KeyRepo::clear_repo`]: crate::repo::key::KeyRepo::clear_repo
    pub fn clear_repo(&mut self) {
        self.0.clear_repo()
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
        let corrupt_keys = self.0.repo.verify()?;
        Ok(self
            .0
            .state
            .hash_table
            .iter()
            .filter(|(_, object_id)| corrupt_keys.contains(&ContentRepoKey::Object(**object_id)))
            .map(|(hash, _)| hash.as_slice())
            .collect::<HashSet<_>>())
    }

    /// Change the password for this repository.
    ///
    /// See [`KeyRepo::change_password`] for details.
    ///
    /// [`KeyRepo::change_password`]: crate::repo::key::KeyRepo::change_password
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.0.repo.change_password(new_password)
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> Uuid {
        self.0.repo.instance()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.0.repo.info()
    }
}
