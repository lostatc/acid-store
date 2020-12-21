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
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

use hex_literal::hex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::key::Key;
use crate::repo::object::{ObjectHandle, ObjectRepo};
use crate::repo::state_helpers::{commit, read_state, rollback, write_state};
use crate::repo::{OpenRepo, RepoInfo, Savepoint};

/// The state for a `ValueRepo`.
#[derive(Debug, Serialize, Deserialize)]
struct ValueRepoState<K: Eq + Hash> {
    key_table: HashMap<K, ObjectHandle>,
}

/// A persistent, heterogeneous, map-like collection.
///
/// See [`crate::repo::value`] for more information.
#[derive(Debug)]
pub struct ValueRepo<K: Key> {
    repo: ObjectRepo,
    state: ValueRepoState<K>,
}

impl<K: Key> OpenRepo for ValueRepo<K> {
    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("d68364ec 4a2a 4a3d 9e80 2832b5dc92c1"));

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
        let state = ValueRepoState {
            key_table: HashMap::new(),
        };
        write_state(&mut repo, &state)?;
        Ok(Self { repo, state })
    }

    fn into_repo(mut self) -> crate::Result<ObjectRepo> {
        write_state(&mut self.repo, &self.state)?;
        Ok(self.repo)
    }
}

impl<K: Key> ValueRepo<K> {
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
        let mut handle = self
            .state
            .key_table
            .entry(key)
            .or_insert(self.repo.add_unmanaged());
        let mut object = self.repo.unmanaged_object_mut(&mut handle).unwrap();
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
            Some(handle) => {
                self.repo.remove_unmanaged(&handle);
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
        let handle = self
            .state
            .key_table
            .get(key)
            .ok_or(crate::Error::NotFound)?;
        let mut object = self.repo.unmanaged_object(&handle).unwrap();
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
        let handle = self
            .state
            .key_table
            .get(source)
            .ok_or(crate::Error::NotFound)?;
        let new_handle = self.repo.copy_unmanaged(&handle);
        self.state.key_table.insert(dest, new_handle);
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

    /// Create a new `Savepoint` representing the current state of the repository.
    ///
    /// See [`ObjectRepo::savepoint`] for details.
    ///
    /// [`ObjectRepo::savepoint`]: crate::repo::object::ObjectRepo::savepoint
    pub fn savepoint(&mut self) -> crate::Result<Savepoint> {
        write_state(&mut self.repo, &self.state)?;
        self.repo.savepoint()
    }

    /// Restore the repository to the given `savepoint`.
    ///
    /// See [`ObjectRepo::restore`] for details.
    ///
    /// [`ObjectRepo::restore`]: crate::repo::object::ObjectRepo::restore
    pub fn restore(&mut self, savepoint: &Savepoint) -> crate::Result<()> {
        self.repo.restore(savepoint)?;
        self.state = read_state(&mut self.repo)?;
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
        for handle in self.state.key_table.values() {
            self.repo.remove_unmanaged(handle);
        }
        self.state.key_table.clear();
    }

    /// Delete all data in all instances of the repository.
    ///
    /// See [`ObjectRepo::clear_repo`] for details.
    ///
    /// [`ObjectRepo::clear_repo`]: crate::repo::object::ObjectRepo::clear_repo
    pub fn clear_repo(&mut self) {
        self.repo.clear_repo();
        self.state.key_table.clear();
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
        let report = self.repo.verify()?;
        Ok(self
            .state
            .key_table
            .iter()
            .filter(|(_, handle)| !report.check_unmanaged(handle))
            .map(|(key, _)| key)
            .collect::<HashSet<_>>())
    }

    /// Change the password for this repository.
    ///
    /// See [`ObjectRepo::change_password`] for details.
    ///
    /// [`ObjectRepo::change_password`]: crate::repo::object::ObjectRepo::change_password
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
