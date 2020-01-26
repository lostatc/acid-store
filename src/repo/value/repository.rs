/*
 * Copyright 2019 Wren Powell
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
use std::hash::Hash;
use std::io::{Read, Write};

use rmp_serde::{from_read, to_vec};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::repo::{Key, LockStrategy, ObjectRepository, RepositoryConfig, RepositoryInfo};
use crate::store::DataStore;

use super::key::{KeyType, ValueKey};

lazy_static! {
    /// The current repository format version ID.
    static ref VERSION_ID: Uuid =
        Uuid::parse_str("5b93b6a4-362f-11ea-b8a5-309c230b49ee ").unwrap();
}

/// A persistent, heterogeneous, map-like collection.
///
/// This is a wrapper around `ObjectRepository` which allows it to map keys to concrete values
/// instead of binary blobs. Values are serialized and deserialized automatically using a
/// space-efficient binary format.
///
/// Like `ObjectRepository`, keys can be of any type implementing `Key`. To access values in the
/// repository, however, a key needs to be wrapped in a `ValueKey`. A `ValueKey` contains type
/// information about the value the key is associated with, allowing for type-safe access to values
/// of different types.
///
/// Like `ObjectRepository`, changes made to the repository are not persisted to disk until `commit`
/// is called. For details about deduplication, compression, encryption, and locking, see
/// `ObjectRepository`.
pub struct ValueRepository<K: Key, S: DataStore> {
    repository: ObjectRepository<KeyType<K>, S>,
}

impl<K: Key, S: DataStore> ValueRepository<K, S> {
    /// Create a new repository backed by the given data `store`.
    ///
    /// See `ObjectRepository::create_repo` for details.
    pub fn create_repo(
        store: S,
        config: RepositoryConfig,
        password: Option<&[u8]>,
    ) -> crate::Result<Self> {
        let mut repository = ObjectRepository::create_repo(store, config, password)?;

        // Write the repository version.
        let mut object = repository.insert(KeyType::Version);
        object.write_all(VERSION_ID.as_bytes())?;
        object.flush()?;
        drop(object);

        Ok(Self { repository })
    }

    /// Open the existing repository in the given data `store`.
    ///
    /// See `ObjectRepository::open_repo` for details.
    pub fn open_repo(
        store: S,
        password: Option<&[u8]>,
        strategy: LockStrategy,
    ) -> crate::Result<Self> {
        let mut repository = ObjectRepository::open_repo(store, password, strategy)?;

        // Read the repository version to see if this is a compatible repository.
        let mut object = repository
            .get(&KeyType::Version)
            .ok_or(crate::Error::NotFound)?;
        let mut version_buffer = Vec::new();
        object.read_to_end(&mut version_buffer)?;
        drop(object);

        let version =
            Uuid::from_slice(version_buffer.as_slice()).map_err(|_| crate::Error::Corrupt)?;
        if version != *VERSION_ID {
            return Err(crate::Error::UnsupportedVersion);
        }

        Ok(Self { repository })
    }

    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        self.repository.contains(&KeyType::Data(key.to_owned()))
    }

    /// Insert a new key-value pair.
    ///
    /// If `key` is already in the repository, its value is replaced.
    ///
    /// # Errors
    /// - `Error::Serialize`: The `value` could not be serialized.
    /// - `Error::Io`: An I/O error occurred.
    pub fn insert<V: Serialize>(&mut self, key: ValueKey<K, V>, value: &V) -> crate::Result<()> {
        let mut object = self.repository.insert(KeyType::Data(key.into_inner()));
        let serialized_value = to_vec(value).map_err(|_| crate::Error::Serialize)?;
        object.write_all(serialized_value.as_slice())?;
        object.flush()?;

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
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        self.repository.remove(&KeyType::Data(key.to_owned()))
    }

    /// Return the value associated with `key`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no value associated with `key`.
    /// - `Error::Deserialize`: The value could not be deserialized.
    /// - `Error::Io`: An I/O error occurred.
    pub fn get<V: DeserializeOwned>(&mut self, key: &ValueKey<K, V>) -> crate::Result<V> {
        let mut object = self
            .repository
            .get(&KeyType::Data(key.get_ref().clone()))
            .ok_or(crate::Error::NotFound)?;
        let mut serialized_value = Vec::new();
        object.read_to_end(&mut serialized_value)?;
        let value =
            from_read(serialized_value.as_slice()).map_err(|_| crate::Error::Deserialize)?;

        Ok(value)
    }

    /// Return a list of all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.repository
            .keys()
            .filter_map(|value_key| match value_key {
                KeyType::Data(key) => Some(key),
                _ => None,
            })
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
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        self.repository
            .copy(&KeyType::Data(source.to_owned()), KeyType::Data(dest))
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        self.repository.commit()
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of keys of values which are corrupt.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&self) -> crate::Result<HashSet<&K>> {
        Ok(self
            .repository
            .verify()?
            .iter()
            .filter_map(|value_key| match value_key {
                KeyType::Data(key) => Some(key),
                _ => None,
            })
            .collect())
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepository::change_password` for details.
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
    pub fn peek_info(store: S) -> crate::Result<RepositoryInfo> {
        ObjectRepository::<K, S>::peek_info(&store)
    }

    /// Consume this repository and return the wrapped `DataStore`.
    pub fn into_store(self) -> S {
        self.repository.into_store()
    }
}
