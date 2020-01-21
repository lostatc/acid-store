/*
 * Copyright 2019 Garrett Powell
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

use std::io::{Read, Write};
use std::time::SystemTime;

use rmp_serde::{from_read, to_vec};
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::repo::{Key, LockStrategy, Object, ObjectRepository, RepositoryConfig, RepositoryInfo};
use crate::store::DataStore;

use super::object::ReadOnlyObject;
use super::version::{Version, VersionKey};

lazy_static! {
    /// The current repository format version ID.
    static ref VERSION_ID: Uuid =
        Uuid::parse_str("0feb0a0e-37ba-11ea-9b23-309c230b49ee").unwrap();
}

/// A persistent object store with support for versioning.
///
/// This is a wrapper around `ObjectRepository` which adds support for storing multiple versions of
/// each object. The current version of each object is mutable, but past versions are read-only.
///
/// Like `ObjectRepository`, changes made to the repository are not persisted to disk until `commit`
/// is called. For details about deduplication, compression, encryption, and locking, see
/// `ObjectRepository`.
pub struct VersionRepository<K: Key, S: DataStore> {
    repository: ObjectRepository<VersionKey<K>, S>,
}

impl<K: Key, S: DataStore> VersionRepository<K, S> {
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
        let mut object = repository.insert(VersionKey::RepositoryVersion);
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
            .get(&VersionKey::RepositoryVersion)
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
    pub fn contains(&self, key: &K) -> bool {
        self.repository.contains(&VersionKey::Object(key.clone()))
    }

    /// Insert the given `key` into the repository and return a new object.
    ///
    /// The returned object represents the current version of the key.
    ///
    /// # Errors
    /// - `Error:AlreadyExists`: The given `key` is already in the repository.
    /// - `Error::Io`: An I/O error occurred.
    pub fn insert(&mut self, key: K) -> crate::Result<Object<VersionKey<K>, S>> {
        if self.contains(&key) {
            return Err(crate::Error::AlreadyExists);
        }

        self.write_versions(&key, &[])?;
        Ok(self.repository.insert(VersionKey::Object(key)))
    }

    /// Remove the given `key` and all its versions from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    ///
    /// # Errors
    /// - `NotFound`: The given `key` is not in the repository.
    /// - `Error::Io`: An I/O error occurred.
    pub fn remove(&mut self, key: &K) -> crate::Result<()> {
        for version in self.list_versions(key)? {
            self.remove_version(key, version.id)?;
        }
        self.repository.remove(&VersionKey::Index(key.clone()));
        self.repository.remove(&VersionKey::Object(key.clone()));

        Ok(())
    }

    /// Return an object for modifying the current version of `key` or `None` if it doesn't exist.
    pub fn get(&mut self, key: &K) -> Option<Object<VersionKey<K>, S>> {
        self.repository.get(&VersionKey::Object(key.clone()))
    }

    /// Return an iterator over all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.repository
            .keys()
            .filter_map(|version_key| match version_key {
                VersionKey::Object(key) => Some(key),
                _ => None,
            })
    }

    /// Create a new version of the given `key`.
    ///
    /// This returns the newly created version.
    ///
    /// # Errors
    /// - `Error::NotFound`: The given `key` is not in the repository.
    /// - `Error::Io`: An I/O error occurred.
    pub fn create_version(&mut self, key: K) -> crate::Result<Version> {
        let mut versions = self.list_versions(&key)?;

        let object = self
            .repository
            .get(&VersionKey::Object(key.clone()))
            .expect("There is no object associated with this key.");
        let size = object.size();
        let content_id = object.content_id();
        drop(object);

        let next_id = versions.iter().map(|version| version.id).max().unwrap_or(0) + 1;
        let new_version = Version {
            id: next_id,
            created: SystemTime::now(),
            size,
            content_id,
        };
        versions.push(new_version.clone());
        self.write_versions(&key, versions.as_slice())?;

        self.repository.copy(
            &VersionKey::Object(key.clone()),
            VersionKey::Version(key.clone(), next_id),
        )?;

        Ok(new_version)
    }

    /// Remove the version of `key` with the given `id`.
    ///
    ///  # Errors
    /// - `Error::NotFound`: There is no version with the given `id`.
    /// - `Error::Io`: An I/O error occurred.
    pub fn remove_version(&mut self, key: &K, id: usize) -> crate::Result<()> {
        self.repository
            .remove(&VersionKey::Version(key.clone(), id));
        let mut versions = self.list_versions(key)?;
        versions.retain(|version| version.id() != id);
        self.write_versions(key, versions.as_slice())?;
        Ok(())
    }

    /// Get an object for reading the version of `key` with the given `id`.
    ///
    /// If there is no version with the given `id`, this returns `None`.
    pub fn get_version(&mut self, key: &K, id: usize) -> Option<ReadOnlyObject<VersionKey<K>, S>> {
        self.repository
            .get(&VersionKey::Version(key.clone(), id))
            .map(|object| object.into())
    }

    /// Return the list of versions of the given `key`.
    ///
    /// # Errors
    /// - `Error::NotFound`: The given key is not in the repository.
    /// - `Error::Io`: An I/O error occurred.
    pub fn list_versions(&mut self, key: &K) -> crate::Result<Vec<Version>> {
        let mut object = self
            .repository
            .get(&VersionKey::Index(key.clone()))
            .ok_or(crate::Error::NotFound)?;

        // Read into a buffer first to catch any I/O errors.
        let mut buffer = Vec::new();
        object.read_to_end(&mut buffer)?;

        Ok(from_read(buffer.as_slice()).expect("Could not deserialize list of versions."))
    }

    /// Write the given `versions` list for the given `key`.
    fn write_versions(&mut self, key: &K, versions: &[Version]) -> crate::Result<()> {
        let mut object = self.repository.insert(VersionKey::Index(key.clone()));
        let serialized_versions = to_vec(versions).expect("Could not serialize list of versions.");
        object.write_all(serialized_versions.as_slice())?;

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See `ObjectRepository::commit` for details.
    pub fn commit(&mut self) -> crate::Result<()> {
        self.repository.commit()
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
        ObjectRepository::<VersionKey<K>, S>::peek_info(&store)
    }
}
