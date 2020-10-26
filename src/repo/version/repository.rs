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
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;
use std::time::SystemTime;

use hex_literal::hex;
use uuid::Uuid;

use super::version::{KeyInfo, Version};
use crate::repo::common::check_version;
use crate::repo::key::Key;
use crate::repo::object::ObjectRepo;
use crate::repo::version::version::VersionInfo;
use crate::repo::{ConvertRepo, Object, ReadOnlyObject, RepoInfo};
use crate::store::DataStore;

/// The ID of the managed object which stores the table of keys for the repository.
const TABLE_OBJECT_ID: Uuid = Uuid::from_bytes(hex!("a2cf16fe bd51 11ea 9785 4be1828714c1"));

/// The current repository format version ID.
///
/// This must be changed any time a backwards-incompatible change is made to the repository
/// format.
const VERSION_ID: Uuid = Uuid::from_bytes(hex!("b1671d9c bd51 11ea ab79 8bcf24ad6a9a"));

/// An object store with support for content versioning.
#[derive(Debug)]
pub struct VersionRepo<K: Key, S: DataStore> {
    repository: ObjectRepo<S>,
    key_table: HashMap<K, KeyInfo>,
}

impl<K: Key, S: DataStore> ConvertRepo<S> for VersionRepo<K, S> {
    fn from_repo(mut repository: ObjectRepo<S>) -> crate::Result<Self> {
        if check_version(&mut repository, VERSION_ID)? {
            // Read and deserialize the key table.
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

    fn into_repo(mut self) -> crate::Result<ObjectRepo<S>> {
        self.commit()?;
        Ok(self.repository)
    }
}

impl<K: Key, S: DataStore> VersionRepo<K, S> {
    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.key_table.contains_key(key)
    }

    /// Insert the given `key` into the repository and return a new object.
    ///
    /// The returned object represents the current version of the key. If the given key already
    /// exists in the repository, this returns `None`.
    pub fn insert(&mut self, key: K) -> Option<Object<S>> {
        if self.key_table.contains_key(&key) {
            return None;
        }

        let handles = KeyInfo {
            versions: BTreeMap::new(),
            object: self.repository.add_unmanaged(),
        };

        let object_handle = &mut self.key_table.entry(key).or_insert(handles).object;
        Some(self.repository.unmanaged_object_mut(object_handle).unwrap())
    }

    /// Remove the given `key` and all its versions from the repository.
    ///
    /// This returns `true` if the key was removed or `false` if it doesn't exist in the repository.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_info = match self.key_table.remove(key) {
            Some(info) => info,
            None => return false,
        };

        for (_, info) in key_info.versions.iter() {
            self.repository.remove_unmanaged(&info.handle);
        }

        self.repository.remove_unmanaged(&key_info.object);

        true
    }

    /// Return a `ReadOnlyObject` for reading the current version of `key`.
    ///
    /// This returns `None` if the key doesn't exist in the repository.
    ///
    /// The returned object provides read-only access to the data. To get read-write access, use
    /// `object_mut`.
    pub fn object<Q>(&self, key: &Q) -> Option<ReadOnlyObject<S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let handle = &self.key_table.get(key)?.object;
        self.repository.unmanaged_object(handle)
    }

    /// Return an `Object` for reading and writing the current version of `key`.
    ///
    /// This returns `None` if the key doesn't exist in the repository.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// `object`.
    pub fn object_mut<Q>(&mut self, key: &Q) -> Option<Object<S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let handle = &mut self.key_table.get_mut(key)?.object;
        self.repository.unmanaged_object_mut(handle)
    }

    /// Return an iterator of all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.key_table.keys()
    }

    /// Create a new version of the given `key` and return it.
    ///
    /// This returns the the newly created version or `None` if the key does not exist in the
    /// repository.
    pub fn create_version<Q>(&mut self, key: &Q) -> Option<Version>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let key_info = self.key_table.get_mut(key)?;

        let version_id = key_info.versions.keys().rev().next().map_or(1, |id| id + 1);
        let version_info = VersionInfo {
            created: SystemTime::now(),
            handle: self.repository.copy_unmanaged(&key_info.object),
        };
        let version = Version {
            id: version_id,
            created: version_info.created,
            content_id: version_info.handle.content_id(),
        };

        key_info.versions.insert(version_id, version_info);

        Some(version)
    }

    /// Remove the version of `key` with the given `version_id`.
    ///
    /// This returns `true` if the version was removed or `false` if it doesn't exist in the
    /// repository.
    pub fn remove_version<Q>(&mut self, key: &Q, version_id: u32) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_info = match self.key_table.get_mut(key) {
            Some(info) => info,
            None => return false,
        };
        let version_info = match key_info.versions.remove(&version_id) {
            Some(info) => info,
            None => return false,
        };

        self.repository.remove_unmanaged(&version_info.handle);

        true
    }

    /// Return an `Object` for reading the contents of a version.
    ///
    /// This returns `None` if the version doesn't exist in the repository.
    ///
    /// # Errors
    /// - `Error::NotFound`: The given `key` is not in the repository.
    /// - `Error::NotFound`: There is no version with the given `version_id`.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn version_object<Q>(&self, key: &Q, version_id: u32) -> Option<ReadOnlyObject<S>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_info = self.key_table.get(key)?;
        let version_info = key_info.versions.get(&version_id)?;
        Some(
            self.repository
                .unmanaged_object(&version_info.handle)
                .unwrap(),
        )
    }

    /// Return the version of `key` with the given `version_id`.
    ///
    /// This returns `None` if the version doesn't exist in the repository.
    pub fn get_version<Q>(&self, key: &Q, version_id: u32) -> Option<Version>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let version_info = self.key_table.get(key)?.versions.get(&version_id)?;
        Some(Version {
            id: version_id,
            created: version_info.created,
            content_id: version_info.handle.content_id(),
        })
    }

    /// Return an iterator of versions for the given `key`.
    ///
    /// This returns `None` if the key doesn't exist in the repository.
    ///
    /// The versions are sorted by their version ID, which corresponds to the order they were
    /// created in.
    pub fn versions<'a, Q>(&'a self, key: &Q) -> Option<impl Iterator<Item = Version> + 'a>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        Some(
            self.key_table
                .get(key)?
                .versions
                .iter()
                .map(|(id, info)| Version {
                    id: *id,
                    created: info.created,
                    content_id: info.handle.content_id(),
                }),
        )
    }

    /// Replace the current version of `key` with the version with the given `version_id`.
    ///
    /// This returns `true` if the version was restored or `false` if the version doesn't exist in
    /// the repository.
    ///
    /// This is a cheap operation which does not require copying the bytes in the object.
    ///
    /// This does not remove the old version.
    pub fn restore_version<Q>(&mut self, key: &Q, version_id: u32) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_info = match self.key_table.get_mut(key) {
            Some(info) => info,
            None => return false,
        };
        let version_info = match key_info.versions.get(&version_id) {
            Some(info) => info,
            None => return false,
        };

        let new_handle = self.repository.copy_unmanaged(&version_info.handle);
        let old_handle = mem::replace(&mut key_info.object, new_handle);
        self.repository.remove_unmanaged(&old_handle);

        true
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepo::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Serialize and write the table of keys.
        let mut object = self.repository.managed_object_mut(TABLE_OBJECT_ID).unwrap();
        object.serialize(&self.key_table)?;
        drop(object);

        // Commit the underlying repository.
        self.repository.commit()
    }

    /// Change the password for this repository.
    ///
    /// See `ObjectRepo::change_password` for details.
    #[cfg(feature = "encryption")]
    pub fn change_password(&mut self, new_password: &[u8]) {
        self.repository.change_password(new_password);
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.repository.info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// See `ObjectRepo::peek_info` for details.
    pub fn peek_info(store: &mut S) -> crate::Result<RepoInfo> {
        ObjectRepo::peek_info(store)
    }
}
