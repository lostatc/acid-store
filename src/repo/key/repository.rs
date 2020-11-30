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
use crate::repo::object::{ObjectHandle, ObjectRepo};
use crate::repo::{ConvertRepo, Object, ReadOnlyObject, RepoInfo};
use crate::store::DataStore;

/// The ID of the managed object which stores the table of keys for the repository.
const TABLE_OBJECT_ID: Uuid = Uuid::from_bytes(hex!("9db2a036 bd2a 11ea 872b c308cda3d138"));

/// The current repository format version ID.
///
/// This must be changed any time a backwards-incompatible change is made to the repository
/// format.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("5dca8ec4 bd3a 11ea bedd 1f70522414fd"));

/// A type which can be used as a key in a `KeyRepo`.
pub trait Key: Eq + Hash + Clone + Serialize + DeserializeOwned {}

impl<T> Key for T where T: Eq + Hash + Clone + Serialize + DeserializeOwned {}

/// An object store which maps keys to seekable binary blobs.
///
/// See [`crate::repo::key`] for more information.
#[derive(Debug)]
pub struct KeyRepo<K: Key> {
    repository: ObjectRepo,
    key_table: HashMap<K, ObjectHandle>,
}

impl<K: Key> ConvertRepo for KeyRepo<K> {
    fn from_repo(mut repository: ObjectRepo) -> crate::Result<Self> {
        if check_version(&mut repository, VERSION_ID)? {
            // Read and deserialize the key table.
            let mut object = repository
                .managed_object(TABLE_OBJECT_ID)
                .ok_or(crate::Error::Corrupt)?;
            let table = object.deserialize()?;

            Ok(Self {
                repository,
                key_table: table,
            })
        } else {
            // Create and write a key table.
            let mut object = repository.add_managed(TABLE_OBJECT_ID);
            let table = HashMap::new();
            object.serialize(&table)?;
            drop(object);

            repository.commit()?;

            Ok(Self {
                repository,
                key_table: table,
            })
        }
    }

    fn into_repo(mut self) -> crate::Result<ObjectRepo> {
        self.repository.rollback()?;
        Ok(self.repository)
    }
}

impl<K: Key> KeyRepo<K> {
    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.key_table.contains_key(key)
    }

    /// Insert the given `key` into the repository and return a new object.
    ///
    /// If the given `key` already exists in the repository, its object is replaced.
    pub fn insert(&mut self, key: K) -> Object {
        self.key_table.remove(&key);
        let handle = self
            .key_table
            .entry(key)
            .or_insert(self.repository.add_unmanaged());
        self.repository.unmanaged_object_mut(handle).unwrap()
    }

    /// Remove the object associated with `key` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        match self.key_table.remove(key) {
            Some(handle) => {
                self.repository.remove_unmanaged(&handle);
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
    /// `object_mut`.
    pub fn object<Q>(&self, key: &Q) -> Option<ReadOnlyObject>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = self.key_table.get(key)?;
        self.repository.unmanaged_object(handle)
    }

    /// Return an `Object` for reading and writing the data associated with `key`.
    ///
    /// This returns `None` if the given key does not exist in the repository.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// `object`.
    pub fn object_mut<Q>(&mut self, key: &Q) -> Option<Object>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = self.key_table.get_mut(key)?;
        self.repository.unmanaged_object_mut(handle)
    }

    /// Return an iterator over all the keys in this repository.
    pub fn keys<'a>(&'a self) -> impl Iterator<Item = &'a K> + 'a {
        self.key_table.keys()
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
        if self.key_table.contains_key(dest.borrow()) {
            return Err(crate::Error::AlreadyExists);
        }

        let handle = self.key_table.get(source).ok_or(crate::Error::NotFound)?;
        let new_handle = self.repository.copy_unmanaged(handle);
        self.key_table.insert(dest, new_handle);

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepo::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Serialize and write the key table.
        let mut object = self.repository.add_managed(TABLE_OBJECT_ID);
        object.serialize(&self.key_table)?;
        drop(object);

        // Commit the underlying repository.
        self.repository.commit()
    }

    /// Roll back all changes made since the last commit.
    ///
    /// See `ObjectRepo::rollback` for details.
    pub fn rollback(&mut self) -> crate::Result<()> {
        // Read and deserialize the key table from the previous commit.
        let mut object = self
            .repository
            .managed_object(TABLE_OBJECT_ID)
            .ok_or(crate::Error::Corrupt)?;
        let key_table = match object.deserialize() {
            Err(crate::Error::Deserialize) => return Err(crate::Error::Corrupt),
            Err(error) => return Err(error),
            Ok(value) => value,
        };
        drop(object);

        self.repository.rollback()?;

        self.key_table = key_table;

        Ok(())
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// See `ObjectRepo::clean` for details.
    pub fn clean(&self) -> crate::Result<()> {
        self.repository.clean()
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of keys of objects which are corrupt.
    ///
    /// If you just need to verify the integrity of one object, `Object::verify` is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
    pub fn verify(&mut self) -> crate::Result<HashSet<&K>> {
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
    /// See `ObjectRepo::change_password` for details.
    #[cfg(feature = "encryption")]
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repository.change_password(new_password)
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> Uuid {
        self.repository.instance()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.repository.info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// See `ObjectRepo::peek_info` for details.
    pub fn peek_info(store: &mut impl DataStore) -> crate::Result<RepoInfo> {
        ObjectRepo::peek_info(store)
    }
}
