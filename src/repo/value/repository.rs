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

use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

use hex_literal::hex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::repo::id_table::UniqueId;
use crate::repo::{
    key::{Key, KeyRepo},
    state_repo::StateRepo,
    OpenRepo, RepoInfo, Savepoint,
};

use super::state::{Restore, ValueRepoKey, ValueRepoState, STATE_KEYS};

/// A persistent, heterogeneous, map-like collection.
///
/// See [`crate::repo::value`] for more information.
#[derive(Debug)]
pub struct ValueRepo<K: Key>(StateRepo<ValueRepoKey, ValueRepoState<K>>);

impl<K: Key> OpenRepo for ValueRepo<K> {
    type Key = ValueRepoKey;

    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("49d1da00 be54 11eb 83e7 ab73adcf2dc4"));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state_repo = StateRepo {
            repo,
            state: ValueRepoState::default(),
            keys: STATE_KEYS,
        };
        state_repo.state = state_repo.read_state()?;
        Ok(ValueRepo(state_repo))
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state_repo = StateRepo {
            repo,
            state: ValueRepoState::default(),
            keys: STATE_KEYS,
        };
        state_repo.write_state()?;
        Ok(ValueRepo(state_repo))
    }

    fn into_repo(mut self) -> crate::Result<KeyRepo<Self::Key>> {
        self.0.write_state()?;
        Ok(self.0.repo)
    }
}

impl<K: Key> ValueRepo<K> {
    /// Remove the object with the given `object_id` from the backing repository.
    fn remove_id(&mut self, object_id: UniqueId) -> bool {
        if !self.0.state.id_table.recycle(object_id) {
            return false;
        }
        if !self.0.repo.remove(&ValueRepoKey::Value(object_id)) {
            panic!("Object ID was in use but not found in backing repository.");
        }
        true
    }

    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.state.key_table.contains_key(key)
    }

    /// Insert a new key-value pair.
    ///
    /// If `key` is already in the repository, its value is replaced.
    ///
    /// # Errors
    /// - `Error::Serialize`: The `value` could not be serialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn insert<V: Serialize>(&mut self, key: K, value: &V) -> crate::Result<()> {
        let object_id = self
            .0
            .state
            .key_table
            .entry(key)
            .or_insert(self.0.state.id_table.next());
        let mut object = self.0.repo.insert(ValueRepoKey::Value(*object_id));
        object.serialize(value)?;
        Ok(())
    }

    /// Remove the value associated with `key` from the repository.
    ///
    /// This returns `true` if the value was removed or `false` if it didn't exist.
    ///
    /// The space used by the given value isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// [`clean`]: crate::repo::value::ValueRepo::clean
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.0.state.key_table.remove(key) {
            Some(object_id) => {
                self.remove_id(object_id);
                true
            }
            None => false,
        }
    }

    /// Return the value associated with `key`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no value associated with `key`.
    /// - `Error::Deserialize`: The value could not be deserialized.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn get<Q, V>(&self, key: &Q) -> crate::Result<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
        V: DeserializeOwned,
    {
        let object_id = self
            .0
            .state
            .key_table
            .get(key)
            .ok_or(crate::Error::NotFound)?;
        let mut object = self
            .0
            .repo
            .object(&ValueRepoKey::Value(*object_id))
            .unwrap();
        object.deserialize()
    }

    /// Return an iterator of all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.0.state.key_table.keys()
    }

    /// Copy the value at `source` to `dest`.
    ///
    /// This is a cheap operation which does not require copying the object itself.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no value at `source`.
    /// - `Error::AlreadyExists`: There is already a value at `dest`.
    pub fn copy<Q>(&mut self, source: &Q, dest: K) -> crate::Result<()>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if self.0.state.key_table.contains_key(dest.borrow()) {
            return Err(crate::Error::AlreadyExists);
        }
        let object_id = self
            .0
            .state
            .key_table
            .get(source)
            .ok_or(crate::Error::NotFound)?;
        let new_object_id = self.0.state.id_table.next();
        self.0.repo.copy(
            &ValueRepoKey::Value(*object_id),
            ValueRepoKey::Value(new_object_id),
        );
        self.0.state.key_table.insert(dest, new_object_id);
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
    pub fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Restore<K>> {
        Ok(Restore(self.0.start_restore(savepoint)?))
    }

    /// Finish the process of restoring the repository to a [`Savepoint`].
    ///
    /// See [`KeyRepo::finish_restore`] for details.
    ///
    /// [`Savepoint`]: crate::repo::Savepoint
    /// [`KeyRepo::finish_restore`]: crate::repo::key::KeyRepo::finish_restore
    pub fn finish_restore(&mut self, restore: Restore<K>) -> bool {
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

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of keys of values which are corrupt.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&self) -> crate::Result<HashSet<&K>> {
        let corrupt_keys = self.0.repo.verify()?;
        Ok(self
            .0
            .state
            .key_table
            .iter()
            .filter(|(_, object_id)| corrupt_keys.contains(&ValueRepoKey::Value(**object_id)))
            .map(|(key, _)| key)
            .collect::<HashSet<_>>())
    }

    /// Change the password for this repository.
    ///
    /// See [`KeyRepo::change_password`] for details.
    ///
    /// [`KeyRepo::change_password`]: crate::repo::key::KeyRepo::change_password
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.0.repo.change_password(new_password);
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
