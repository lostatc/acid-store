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
use std::fmt::Debug;
use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::repo::common::check_version;
use crate::repo::object::{ObjectHandle, ObjectRepository};
use crate::repo::{ConvertRepo, Object, ReadOnlyObject, RepositoryInfo};
use crate::store::DataStore;
use std::borrow::Borrow;

lazy_static! {
    /// The ID of the managed object which stores the table of keys for the repository.
    static ref TABLE_OBJECT_ID: Uuid =
        Uuid::parse_str("9db2a036-bd2a-11ea-872b-c308cda3d138").unwrap();

    /// The current repository format version ID.
    ///
    /// This must be changed any time a backwards-incompatible change is made to the repository
    /// format.
    static ref VERSION_ID: Uuid =
        Uuid::parse_str("5dca8ec4-bd3a-11ea-bedd-1f70522414fd").unwrap();
}

/// A type which can be used as a key in a `KeyRepository`.
pub trait Key: Eq + Hash + Clone + Serialize + DeserializeOwned {}

impl<T> Key for T where T: Eq + Hash + Clone + Serialize + DeserializeOwned {}

/// An object store which maps keys to seekable binary blobs.
///
/// A `KeyRepository` maps keys of type `K` to seekable binary blobs called objects and stores
/// them persistently in a `DataStore`.
///
/// Like other repositories, changes made to the repository are not persisted to the data store
/// until `commit` is called. For details about deduplication, compression, encryption, and locking,
/// see the module-level documentation for `acid_store::repo`.
#[derive(Debug)]
pub struct KeyRepository<K: Key, S: DataStore> {
    repository: ObjectRepository<S>,
    key_table: HashMap<K, ObjectHandle>,
}

impl<K: Key, S: DataStore> ConvertRepo<S> for KeyRepository<K, S> {
    fn from_repo(mut repository: ObjectRepository<S>) -> crate::Result<Self> {
        if check_version(&mut repository, *VERSION_ID)? {
            // Read and deserialize the key table.
            let mut object = repository
                .managed_object(*TABLE_OBJECT_ID)
                .ok_or(crate::Error::Corrupt)?;
            let table = object.deserialize()?;

            Ok(Self {
                repository,
                key_table: table,
            })
        } else {
            // Create and write a key table.
            let mut object = repository.add_managed(*TABLE_OBJECT_ID);
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

    fn into_repo(mut self) -> crate::Result<ObjectRepository<S>> {
        self.commit()?;
        Ok(self.repository)
    }
}

impl<K: Key, S: DataStore> KeyRepository<K, S> {
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
    pub fn insert(&mut self, key: K) -> Object<S> {
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
    pub fn object<Q>(&self, key: &Q) -> Option<ReadOnlyObject<S>>
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
    pub fn object_mut<Q>(&mut self, key: &Q) -> Option<Object<S>>
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
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Serialize and write the key table.
        let mut object = self.repository.add_managed(*TABLE_OBJECT_ID);
        object.serialize(&self.key_table)?;
        drop(object);

        // Commit the underlying repository.
        self.repository.commit()
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of keys of objects which are corrupt.
    ///
    /// If you just need to verify the integrity of one object, `Object::verify` is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
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
        self.repository.change_password(new_password)
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
