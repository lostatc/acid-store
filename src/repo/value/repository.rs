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
    state_repo, OpenRepo, RepoInfo, Savepoint,
};

use super::state::{Restore, ValueRepoKey, ValueRepoState, STATE_KEYS};

/// A persistent, heterogeneous, map-like collection.
///
/// See [`crate::repo::value`] for more information.
#[derive(Debug)]
pub struct ValueRepo<K: Key> {
    repo: KeyRepo<ValueRepoKey>,
    state: ValueRepoState<K>,
}

impl<K: Key> OpenRepo for ValueRepo<K> {
    type Key = ValueRepoKey;

    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("49d1da00 be54 11eb 83e7 ab73adcf2dc4"));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut value_repo = Self {
            repo,
            state: ValueRepoState::new(),
        };
        value_repo.state = value_repo.read_state()?;
        Ok(value_repo)
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut value_repo = Self {
            repo,
            state: ValueRepoState::new(),
        };
        value_repo.write_state()?;
        Ok(value_repo)
    }

    fn into_repo(mut self) -> crate::Result<KeyRepo<Self::Key>> {
        self.write_state()?;
        Ok(self.repo)
    }
}

impl<K: Key> ValueRepo<K> {
    /// Read the current repository state from the backing repository and return it.
    fn read_state(&mut self) -> crate::Result<ValueRepoState<K>> {
        state_repo::read_state(&mut self.repo, STATE_KEYS)
    }

    /// Write the current repository state to the backing repository.
    fn write_state(&mut self) -> crate::Result<()> {
        state_repo::write_state(&mut self.repo, STATE_KEYS, &self.state)
    }

    /// Remove the object with the given `object_id` from the backing repository.
    fn remove_id(&mut self, object_id: UniqueId) -> bool {
        if !self.state.id_table.recycle(object_id) {
            return false;
        }
        if !self.repo.remove(&ValueRepoKey::Value(object_id)) {
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
        self.state.key_table.contains_key(key)
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
            .state
            .key_table
            .entry(key)
            .or_insert(self.state.id_table.next());
        let mut object = self.repo.insert(ValueRepoKey::Value(*object_id));
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
        match self.state.key_table.remove(key) {
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
            .state
            .key_table
            .get(key)
            .ok_or(crate::Error::NotFound)?;
        let mut object = self.repo.object(&ValueRepoKey::Value(*object_id)).unwrap();
        object.deserialize()
    }

    /// Return an iterator of all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.state.key_table.keys()
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
        if self.state.key_table.contains_key(dest.borrow()) {
            return Err(crate::Error::AlreadyExists);
        }
        let object_id = self
            .state
            .key_table
            .get(source)
            .ok_or(crate::Error::NotFound)?;
        let new_object_id = self.state.id_table.next();
        self.repo.copy(
            &ValueRepoKey::Value(*object_id),
            ValueRepoKey::Value(new_object_id),
        );
        self.state.key_table.insert(dest, new_object_id);
        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See [`KeyRepo::commit`] for details.
    ///
    /// [`KeyRepo::commit`]: crate::repo::key::KeyRepo::commit
    pub fn commit(&mut self) -> crate::Result<()> {
        state_repo::commit(&mut self.repo, STATE_KEYS, &self.state)
    }

    /// Roll back all changes made since the last commit.
    ///
    /// See [`KeyRepo::rollback`] for details.
    ///
    /// [`KeyRepo::rollback`]: crate::repo::key::KeyRepo::rollback
    pub fn rollback(&mut self) -> crate::Result<()> {
        state_repo::rollback(&mut self.repo, STATE_KEYS, &mut self.state)
    }

    /// Create a new `Savepoint` representing the current state of the repository.
    ///
    /// See [`KeyRepo::savepoint`] for details.
    ///
    /// [`KeyRepo::savepoint`]: crate::repo::key::KeyRepo::savepoint
    pub fn savepoint(&mut self) -> crate::Result<Savepoint> {
        state_repo::savepoint(&mut self.repo, STATE_KEYS, &self.state)
    }

    /// Start the process of restoring the repository to the given `savepoint`.
    ///
    /// See [`KeyRepo::start_restore`] for details.
    ///
    /// [`KeyRepo::start_restore`]: crate::repo::key::KeyRepo::start_restore
    pub fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Restore<K>> {
        Ok(Restore(state_repo::start_restore(
            &mut self.repo,
            STATE_KEYS,
            savepoint,
        )?))
    }

    /// Finish the process of restoring the repository to a [`Savepoint`].
    ///
    /// See [`KeyRepo::finish_restore`] for details.
    ///
    /// [`Savepoint`]: crate::repo::Savepoint
    /// [`KeyRepo::finish_restore`]: crate::repo::key::KeyRepo::finish_restore
    pub fn finish_restore(&mut self, restore: Restore<K>) -> bool {
        state_repo::finish_restore(&mut self.repo, &mut self.state, restore.0)
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// See [`KeyRepo::clean`] for details.
    ///
    /// [`KeyRepo::clean`]: crate::repo::key::KeyRepo::clean
    pub fn clean(&mut self) -> crate::Result<()> {
        self.repo.clean()
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.repo.clear_instance();
        self.state.clear();
    }

    /// Delete all data in all instances of the repository.
    ///
    /// See [`KeyRepo::clear_repo`] for details.
    ///
    /// [`KeyRepo::clear_repo`]: crate::repo::key::KeyRepo::clear_repo
    pub fn clear_repo(&mut self) {
        self.repo.clear_repo();
        self.state.clear();
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
        let corrupt_keys = self.repo.verify()?;
        Ok(self
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
        self.repo.change_password(new_password);
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
