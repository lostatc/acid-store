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

use crate::repo::object::{ObjectHandle, ObjectRepo};
use crate::repo::state_helpers::{commit, read_state, rollback, write_state};
use crate::repo::{Object, OpenRepo, ReadOnlyObject, RepoInfo, Savepoint};

/// A type which can be used as a key in a `KeyRepo`.
pub trait Key: Eq + Hash + Clone + Serialize + DeserializeOwned {}

impl<T> Key for T where T: Eq + Hash + Clone + Serialize + DeserializeOwned {}

/// The state for a `KeyRepo`.
#[derive(Debug, Serialize, Deserialize)]
struct KeyRepoState<K: Eq + Hash> {
    key_table: HashMap<K, ObjectHandle>,
}

/// An object store which maps keys to seekable binary blobs.
///
/// See [`crate::repo::key`] for more information.
#[derive(Debug)]
pub struct KeyRepo<K: Key> {
    repo: ObjectRepo,
    state: KeyRepoState<K>,
}

impl<K: Key> OpenRepo for KeyRepo<K> {
    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("2a48cbfe 458b 433d ad20 4573e72a33ad"));

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
        let state = KeyRepoState {
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

impl<K: Key> KeyRepo<K> {
    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.state.key_table.contains_key(key)
    }

    /// Insert the given `key` into the repository and return a new object.
    ///
    /// If the given `key` already exists in the repository, its object is replaced.
    pub fn insert(&mut self, key: K) -> Object {
        self.state.key_table.remove(&key);
        let handle = self
            .state
            .key_table
            .entry(key)
            .or_insert(self.repo.add_unmanaged());
        self.repo.unmanaged_object_mut(handle).unwrap()
    }

    /// Remove the object associated with `key` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// [`clean`]: crate::repo::key::KeyRepo::clean
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        match self.state.key_table.remove(key) {
            Some(handle) => {
                self.repo.remove_unmanaged(&handle);
                true
            }
            None => false,
        }
    }

    /// Return a `ReadOnlyObject` for reading the data associated with `key`.
    ///
    /// This returns `None` if the given key does not exist in the repository.
    ///
    /// The returned object provides read-only access to the data. To get read-write access, use
    /// [`object_mut`].
    ///
    /// [`object_mut`]: crate::repo::key::KeyRepo::object_mut
    pub fn object<Q>(&self, key: &Q) -> Option<ReadOnlyObject>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = self.state.key_table.get(key)?;
        self.repo.unmanaged_object(handle)
    }

    /// Return an `Object` for reading and writing the data associated with `key`.
    ///
    /// This returns `None` if the given key does not exist in the repository.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// [`object`].
    ///
    /// [`object`]: crate::repo::key::KeyRepo::object
    pub fn object_mut<Q>(&mut self, key: &Q) -> Option<Object>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = self.state.key_table.get_mut(key)?;
        self.repo.unmanaged_object_mut(handle)
    }

    /// Return an iterator over all the keys in this repository.
    pub fn keys<'a>(&'a self) -> impl Iterator<Item = &'a K> + 'a {
        self.state.key_table.keys()
    }

    /// Copy the object at `source` to `dest`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the object.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no object at `source`.
    /// - `Error::AlreadyExists`: There is already an object at `dest`.
    pub fn copy<Q>(&mut self, source: &Q, dest: K) -> crate::Result<()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if self.state.key_table.contains_key(dest.borrow()) {
            return Err(crate::Error::AlreadyExists);
        }

        let handle = self
            .state
            .key_table
            .get(source)
            .ok_or(crate::Error::NotFound)?;
        let new_handle = self.repo.copy_unmanaged(handle);
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
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`ObjectRepo::savepoint`]: crate::repo::object::ObjectRepo::savepoint
    pub fn savepoint(&mut self) -> crate::Result<Savepoint> {
        write_state(&mut self.repo, &self.state)?;
        Ok(self.repo.savepoint())
    }

    /// Restore the repository to the given `savepoint`.
    ///
    /// See [`ObjectRepo::restore`] for details.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`ObjectRepo::restore`]: crate::repo::object::ObjectRepo::restore
    pub fn restore(&mut self, savepoint: &Savepoint) -> crate::Result<bool> {
        if !self.repo.restore(savepoint) {
            return Ok(false);
        }
        self.state = read_state(&mut self.repo)?;
        Ok(true)
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
    /// This does not delete data from other instances of the repository.
    ///
    /// No data is reclaimed in the backing data store until changes are committed and [`clean`] is
    /// called.
    ///
    /// [`clean`]: crate::repo::key::KeyRepo::clean
    pub fn clear_instance(&mut self) {
        for handle in self.state.key_table.values() {
            self.repo.remove_unmanaged(handle);
        }
        self.state.key_table.clear()
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
    /// This returns the set of keys of objects which are corrupt.
    ///
    /// If you just need to verify the integrity of one object, [`Object::verify`] is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
    ///
    /// [`Object::verify`]: crate::repo::Object::verify
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
