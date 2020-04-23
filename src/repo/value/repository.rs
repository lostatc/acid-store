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
use std::io::{Read, Write};

use rmp_serde::{from_read, to_vec};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::repo::key_id::KeyTable;
use crate::repo::version_id::{check_version, write_version};
use crate::repo::{
    Key, LockStrategy, ObjectRepository, OpenRepo, RepositoryConfig, RepositoryInfo,
    RepositoryStats,
};
use crate::store::DataStore;

use super::key::ValueKey;

lazy_static! {
    /// The current repository format version ID.
    static ref VERSION_ID: Uuid =
        Uuid::parse_str("4d99cf79-ffbd-43dc-9399-b03b5f0bad00").unwrap();
}

/// A persistent, heterogeneous, map-like collection.
///
/// This is a wrapper around `ObjectRepository` which allows it to map keys to concrete values
/// instead of binary blobs. Values are serialized and deserialized automatically using a
/// space-efficient binary format.
///
/// Like `ObjectRepository`, changes made to the repository are not persisted to the data store
/// until `commit` is called. For details about deduplication, compression, encryption, and locking,
/// see `ObjectRepository`.
#[derive(Debug)]
pub struct ValueRepository<K: Key, S: DataStore> {
    repository: ObjectRepository<ValueKey, S>,
    key_table: KeyTable<K>,
}

impl<K: Key, S: DataStore> OpenRepo<S> for ValueRepository<K, S> {
    fn open_repo(store: S, strategy: LockStrategy, password: Option<&[u8]>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let repository = ObjectRepository::open_repo(store, strategy, password)?;

        // Read the repository version to see if this is a compatible repository.
        let object = repository
            .get(&ValueKey::RepositoryVersion)
            .ok_or(crate::Error::NotFound)?;
        check_version(object, *VERSION_ID)?;

        // Read and deserialize the key table.
        let object = repository
            .get(&ValueKey::KeyTable)
            .ok_or(crate::Error::Corrupt)?;
        let key_table = KeyTable::read(object)?;

        Ok(Self {
            repository,
            key_table,
        })
    }

    fn new_repo(store: S, config: RepositoryConfig, password: Option<&[u8]>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut repository = ObjectRepository::new_repo(store, config, password)?;

        // Write the repository version.
        let object = repository.insert(ValueKey::RepositoryVersion);
        write_version(object, *VERSION_ID)?;

        // Create and write a key table.
        let object = repository.insert(ValueKey::KeyTable);
        let key_table = KeyTable::new();
        key_table.write(object)?;

        repository.commit()?;

        Ok(Self {
            repository,
            key_table,
        })
    }

    fn create_repo(
        mut store: S,
        config: RepositoryConfig,
        strategy: LockStrategy,
        password: Option<&[u8]>,
    ) -> crate::Result<Self>
    where
        Self: Sized,
    {
        if store.list_blocks().map_err(anyhow::Error::from)?.is_empty() {
            Self::new_repo(store, config, password)
        } else {
            Self::open_repo(store, strategy, password)
        }
    }
}

impl<K: Key, S: DataStore> ValueRepository<K, S> {
    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.key_table.contains(key)
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
        let key_id = self.key_table.insert(key);
        let mut object = self.repository.insert(ValueKey::Data(key_id));
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
        Q: Hash + Eq + ?Sized,
    {
        match self.key_table.remove(key) {
            Some(key_id) => self.repository.remove(&ValueKey::Data(key_id)),
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
        let key_id = self.key_table.get(key).ok_or(crate::Error::NotFound)?;
        let mut object = self
            .repository
            .get(&ValueKey::Data(key_id))
            .ok_or(crate::Error::NotFound)?;

        // Catch any errors before passing to `from_read`.
        let mut serialized_value = Vec::with_capacity(object.size() as usize);
        object.read_to_end(&mut serialized_value)?;

        let value =
            from_read(serialized_value.as_slice()).map_err(|_| crate::Error::Deserialize)?;

        Ok(value)
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
        let source_key_id = self.key_table.get(source).ok_or(crate::Error::NotFound)?;
        let dest_key_id = self.key_table.insert(dest);
        self.repository
            .copy(&ValueKey::Data(source_key_id), ValueKey::Data(dest_key_id))
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        let object = self
            .repository
            .get_mut(&ValueKey::KeyTable)
            .expect("This repository has no key table.");
        self.key_table.write(object)?;
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
        let corrupt_key_ids = self
            .repository
            .verify()?
            .iter()
            .filter_map(|value_key| match value_key {
                ValueKey::Data(key) => Some(key),
                _ => None,
            })
            .collect::<HashSet<_>>();

        Ok(self
            .key_table
            .iter()
            .filter_map(|(key, key_id)| {
                if corrupt_key_ids.contains(&key_id) {
                    Some(key)
                } else {
                    None
                }
            })
            .collect())
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
        ObjectRepository::<K, S>::peek_info(store)
    }

    /// Calculate statistics about the repository.
    pub fn stats(&self) -> RepositoryStats {
        self.repository.stats()
    }

    /// Consume this repository and return the wrapped `DataStore`.
    pub fn into_store(self) -> S {
        self.repository.into_store()
    }
}
