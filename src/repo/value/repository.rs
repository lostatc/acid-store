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

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use hex_literal::hex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::repo::{
    key::{Key, KeyRepo},
    state::{ObjectId, StateRepo},
    Commit, OpenRepo, RepoInfo, RestoreSavepoint, Savepoint,
};

type RepoState<K> = HashMap<K, ObjectId>;

/// A persistent, heterogeneous, map-like collection.
///
/// See [`crate::repo::value`] for more information.
#[derive(Debug)]
pub struct ValueRepo<K: Key>(StateRepo<RepoState<K>>);

impl<K: Key> OpenRepo for ValueRepo<K> {
    type Key = <StateRepo<RepoState<K>> as OpenRepo>::Key;

    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("4db4c84c cfc7 11eb 9e06 77121c3277f7"));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self(StateRepo::open_repo(repo)?))
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self(StateRepo::create_repo(repo)?))
    }

    fn into_repo(self) -> crate::Result<KeyRepo<Self::Key>> {
        self.0.into_repo()
    }
}

impl<K: Key> ValueRepo<K> {
    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.state().contains_key(key)
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
        let object_id = self.0.create();
        let mut object = self.0.object(object_id).unwrap();
        let result = object.serialize(value);
        drop(object);
        if let Err(error) = result {
            self.0.remove(object_id);
            return Err(error);
        }

        if let Some(prev_object_id) = self.0.state_mut().insert(key, object_id) {
            self.0.remove(prev_object_id);
        }

        Ok(())
    }

    /// Remove the value associated with `key` from the repository.
    ///
    /// This returns `true` if the value was removed or `false` if it didn't exist.
    ///
    /// The space used by the given value isn't reclaimed in the backing data store until changes
    /// are committed and [`Commit::clean`] is called.
    ///
    /// [`Commit::clean`]: crate::repo::Commit::clean
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.0.state_mut().remove(key) {
            Some(object_id) => {
                self.0.remove(object_id);
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
        let object_id = self.0.state().get(key).ok_or(crate::Error::NotFound)?;
        let mut object = self.0.object(*object_id).unwrap();
        object.deserialize()
    }

    /// Return an iterator of all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.0.state().keys()
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
        if self.0.state().contains_key(dest.borrow()) {
            return Err(crate::Error::AlreadyExists);
        }
        let object_id = *self.0.state().get(source).ok_or(crate::Error::NotFound)?;
        let new_object_id = self.0.copy(object_id).unwrap();
        self.0.state_mut().insert(dest, new_object_id);
        Ok(())
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
        let corrupt_keys = self.0.verify()?;
        Ok(self
            .0
            .state()
            .iter()
            .filter(|(_, object_id)| corrupt_keys.contains(*object_id))
            .map(|(key, _)| key)
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
        self.0.change_password(new_password);
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> Uuid {
        self.0.instance()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.0.info()
    }
}

impl<K: Key> Commit for ValueRepo<K> {
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

impl<K: Key> RestoreSavepoint for ValueRepo<K> {
    type Restore = <StateRepo<RepoState<K>> as RestoreSavepoint>::Restore;

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
