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
use serde::Serialize;
use uuid::Uuid;

use crate::repo::common::check_version;
use crate::repo::key::Key;
use crate::repo::object::{ObjectHandle, ObjectRepository};
use crate::repo::{ConvertRepo, RepositoryInfo};
use crate::store::DataStore;

/// The ID of the managed object which stores the table of keys for the repository.
const TABLE_OBJECT_ID: Uuid = Uuid::from_bytes(hex!("69f329d6 bd4e 11ea 980a 3f2f192c2e86"));

/// The current repository format version ID.
///
/// This must be changed any time a backwards-incompatible change is made to the repository
/// format.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("7457459c bd4e 11ea 8dad 67ac9eea7160"));

/// A persistent, heterogeneous, map-like collection.
///
/// This is a repository which maps keys to concrete values instead of binary blobs. Values are
/// serialized and deserialized automatically using a space-efficient binary format.
///
/// Like other repositories, changes made to the repository are not persisted to the data store
/// until `commit` is called. For details about deduplication, compression, encryption, and locking,
/// see the module-level documentation for `acid_store::repo`.
#[derive(Debug)]
pub struct ValueRepository<K: Key, S: DataStore> {
    repository: ObjectRepository<S>,
    key_table: HashMap<K, ObjectHandle>,
}

impl<K: Key, S: DataStore> ConvertRepo<S> for ValueRepository<K, S> {
    fn from_repo(mut repository: ObjectRepository<S>) -> crate::Result<Self> {
        if check_version(&mut repository, VERSION_ID)? {
            // Read and deserialize the table of keys.
            let mut object = repository
                .managed_object(TABLE_OBJECT_ID)
                .ok_or(crate::Error::Corrupt)?;
            let key_table = object.deserialize()?;

            Ok(Self {
                repository,
                key_table,
            })
        } else {
            // Create and write the table of keys.
            let mut object = repository.add_managed(TABLE_OBJECT_ID);
            let key_table = HashMap::new();
            object.serialize(&key_table)?;
            drop(object);

            repository.commit()?;

            Ok(Self {
                repository,
                key_table,
            })
        }
    }

    fn into_repo(mut self) -> crate::Result<ObjectRepository<S>> {
        self.commit()?;
        Ok(self.repository)
    }
}

impl<K: Key, S: DataStore> ValueRepository<K, S> {
    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.key_table.contains_key(key)
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
            .key_table
            .entry(key)
            .or_insert(self.repository.add_unmanaged());
        let mut object = self.repository.unmanaged_object_mut(&mut handle).unwrap();
        object.serialize(value)?;
        Ok(())
    }

    /// Remove the value associated with `key` from the repository.
    ///
    /// This returns `true` if the value was removed or `false` if it didn't exist.
    ///
    /// The space used by the given value isn't freed and made available for new values until
    /// `commit` is called.
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.key_table.remove(key) {
            Some(handle) => {
                self.repository.remove_unmanaged(&handle);
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
        let handle = self.key_table.get(key).ok_or(crate::Error::NotFound)?;
        let mut object = self.repository.unmanaged_object(&handle).unwrap();
        object.deserialize()
    }

    /// Return an iterator of all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.key_table.keys()
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
        if self.key_table.contains_key(dest.borrow()) {
            return Err(crate::Error::AlreadyExists);
        }
        let handle = self.key_table.get(source).ok_or(crate::Error::NotFound)?;
        let new_handle = self.repository.copy_unmanaged(&handle);
        self.key_table.insert(dest, new_handle);
        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Serialize and write the table of keys.
        let mut object = self.repository.managed_object_mut(TABLE_OBJECT_ID).unwrap();
        object.serialize(&self.key_table)?;
        drop(object);

        // Commit the underlying repository.
        self.repository.commit()
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
        let report = self.repository.verify()?;
        Ok(self
            .key_table
            .iter()
            .filter(|(_, handle)| !report.check_unmanaged(handle))
            .map(|(key, _)| key)
            .collect::<HashSet<_>>())
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepository::change_password` for details.
    #[cfg(feature = "encryption")]
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repository.change_password(new_password);
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepositoryInfo {
        self.repository.info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// See `ObjectRepository::peek_info` for details.
    pub fn peek_info(store: &mut S) -> crate::Result<RepositoryInfo> {
        ObjectRepository::peek_info(store)
    }
}
