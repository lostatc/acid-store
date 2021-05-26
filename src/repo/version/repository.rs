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
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;
use std::time::SystemTime;

use hex_literal::hex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::id_table::UniqueId;
use crate::repo::{
    key::{Key, KeyRepo},
    state_repo, Object, OpenRepo, ReadOnlyObject, RepoInfo, Savepoint,
};

use super::state::{Restore, VersionRepoKey, VersionRepoState, STATE_KEYS};
use super::version::{KeyInfo, Version, VersionInfo};

/// An object store with support for content versioning.
///
/// See [`crate::repo::version`] for more information.
#[derive(Debug)]
pub struct VersionRepo<K: Key> {
    repo: KeyRepo<VersionRepoKey>,
    state: VersionRepoState<K>,
}

impl<K: Key> OpenRepo for VersionRepo<K> {
    type Key = VersionRepoKey;

    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("590bd584 be86 11eb b54d c32102ab5ae4"));

    fn open_repo(mut repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut version_repo = Self {
            repo,
            state: VersionRepoState::new(),
        };
        version_repo.state = version_repo.read_state()?;
        Ok(version_repo)
    }

    fn create_repo(mut repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut version_repo = Self {
            repo,
            state: VersionRepoState::new(),
        };
        version_repo.write_state()?;
        Ok(version_repo)
    }

    fn into_repo(mut self) -> crate::Result<KeyRepo<Self::Key>> {
        self.write_state()?;
        Ok(self.repo)
    }
}

impl<K: Key> VersionRepo<K> {
    /// Read the current repository state from the backing repository and return it.
    fn read_state(&mut self) -> crate::Result<VersionRepoState<K>> {
        state_repo::read_state(&mut self.repo, STATE_KEYS)
    }

    /// Write the current repository state to the backing repository.
    fn write_state(&mut self) -> crate::Result<()> {
        state_repo::write_state(&mut self.repo, STATE_KEYS, &self.state)
    }

    /// Remove the object with the given `object_id` from the backing repository.
    fn remove_id(&mut self, object_id: UniqueId) -> bool {
        if !self.state.id_table.recycle(object_id) {
            return false;
        }
        if !self.repo.remove(&VersionRepoKey::Version(object_id)) {
            panic!("Object ID was in use but not found in backing repository.");
        }
        true
    }

    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.state.key_table.contains_key(key)
    }

    /// Insert the given `key` into the repository and return a new object.
    ///
    /// The returned object represents the current version of the key. If the given key already
    /// exists in the repository, this returns `None`.
    pub fn insert(&mut self, key: K) -> Option<Object> {
        if self.state.key_table.contains_key(&key) {
            return None;
        }

        let key_info = KeyInfo {
            versions: BTreeMap::new(),
            object: self.state.id_table.next(),
        };

        let object_id = &mut self.state.key_table.entry(key).or_insert(key_info).object;
        Some(self.repo.insert(VersionRepoKey::Version(*object_id)))
    }

    /// Remove the given `key` and all its versions from the repository.
    ///
    /// This returns `true` if the key was removed or `false` if it doesn't exist in the repository.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// [`clean`]: crate::repo::version::VersionRepo::clean
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_info = match self.state.key_table.remove(key) {
            Some(info) => info,
            None => return false,
        };

        for (_, info) in key_info.versions.iter() {
            self.remove_id(info.id);
        }

        self.remove_id(key_info.object);

        true
    }

    /// Return a `ReadOnlyObject` for reading the current version of `key`.
    ///
    /// This returns `None` if the key doesn't exist in the repository.
    ///
    /// The returned object provides read-only access to the data. To get read-write access, use
    /// [`object_mut`].
    ///
    /// [`object_mut`]: crate::repo::version::VersionRepo::object_mut
    pub fn object<Q>(&self, key: &Q) -> Option<ReadOnlyObject>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let object_id = &self.state.key_table.get(key)?.object;
        Some(
            self.repo
                .object(&VersionRepoKey::Version(*object_id))
                .unwrap(),
        )
    }

    /// Return an `Object` for reading and writing the current version of `key`.
    ///
    /// This returns `None` if the key doesn't exist in the repository.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// [`object`].
    ///
    /// [`object`]: crate::repo::version::VersionRepo::object
    pub fn object_mut<Q>(&mut self, key: &Q) -> Option<Object>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let object_id = &self.state.key_table.get(key)?.object;
        Some(
            self.repo
                .object_mut(&VersionRepoKey::Version(*object_id))
                .unwrap(),
        )
    }

    /// Return an iterator of all the keys in this repository.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.state.key_table.keys()
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
        let key_info = self.state.key_table.get_mut(key)?;

        let version_id = key_info.versions.keys().rev().next().map_or(1, |id| id + 1);

        let new_object_id = self.state.id_table.next();
        self.repo.copy(
            &VersionRepoKey::Version(key_info.object),
            VersionRepoKey::Version(new_object_id),
        );

        let version_info = VersionInfo {
            created: SystemTime::now(),
            id: new_object_id,
        };

        let version = Version {
            id: version_id,
            created: version_info.created,
            content_id: self
                .repo
                .object(&VersionRepoKey::Version(version_info.id))
                .unwrap()
                .content_id(),
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
        let key_info = match self.state.key_table.get_mut(key) {
            Some(info) => info,
            None => return false,
        };
        let version_info = match key_info.versions.remove(&version_id) {
            Some(info) => info,
            None => return false,
        };

        self.remove_id(version_info.id);

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
    pub fn version_object<Q>(&self, key: &Q, version_id: u32) -> Option<ReadOnlyObject>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_info = self.state.key_table.get(key)?;
        let version_info = key_info.versions.get(&version_id)?;
        Some(
            self.repo
                .object(&VersionRepoKey::Version(version_info.id))
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
        let version_info = self.state.key_table.get(key)?.versions.get(&version_id)?;
        Some(Version {
            id: version_id,
            created: version_info.created,
            content_id: self
                .repo
                .object(&VersionRepoKey::Version(version_info.id))
                .unwrap()
                .content_id(),
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
        let versions = &self.state.key_table.get(key)?.versions;
        Some(versions.iter().map(move |(id, info)| {
            Version {
                id: *id,
                created: info.created,
                content_id: self
                    .repo
                    .object(&VersionRepoKey::Version(info.id))
                    .unwrap()
                    .content_id(),
            }
        }))
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
        let key_info = match self.state.key_table.get_mut(key) {
            Some(info) => info,
            None => return false,
        };
        let version_info = match key_info.versions.get(&version_id) {
            Some(info) => info,
            None => return false,
        };

        let new_object_id = self.state.id_table.next();
        self.repo.copy(
            &VersionRepoKey::Version(version_info.id),
            VersionRepoKey::Version(new_object_id),
        );
        let old_object_id = mem::replace(&mut key_info.object, new_object_id);
        self.remove_id(old_object_id);

        true
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See [`KeyRepo::commit`] for details.
    ///
    /// [`KeyRepo::commit`]: crate::repo::key::KeyRepo::commit
    pub fn commit(&mut self) -> crate::Result<()> {
        state_repo::commit(&mut self.repo, STATE_KEYS, &self.state)
    }

    /// Roll back all changes made since the last commit.
    ///
    /// See [`KeyRepo::rollback`] for details.
    ///
    /// [`KeyRepo::rollback`]: crate::repo::key::KeyRepo::rollback
    pub fn rollback(&mut self) -> crate::Result<()> {
        state_repo::rollback(&mut self.repo, STATE_KEYS, &mut self.state)
    }

    /// Create a new `Savepoint` representing the current state of the repository.
    ///
    /// See [`KeyRepo::savepoint`] for details.
    ///
    /// [`KeyRepo::savepoint`]: crate::repo::key::KeyRepo::savepoint
    pub fn savepoint(&mut self) -> crate::Result<Savepoint> {
        state_repo::savepoint(&mut self.repo, STATE_KEYS, &self.state)
    }

    /// Start the process of restoring the repository to the given `savepoint`.
    ///
    /// See [`KeyRepo::start_restore`] for details.
    ///
    /// [`KeyRepo::start_restore`]: crate::repo::key::KeyRepo::start_restore
    pub fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Restore<K>> {
        Ok(Restore(state_repo::start_restore(
            &mut self.repo,
            STATE_KEYS,
            savepoint,
        )?))
    }

    /// Finish the process of restoring the repository to a [`Savepoint`].
    ///
    /// See [`KeyRepo::finish_restore`] for details.
    ///
    /// [`Savepoint`]: crate::repo::Savepoint
    /// [`KeyRepo::finish_restore`]: crate::repo::key::KeyRepo::finish_restore
    pub fn finish_restore(&mut self, restore: Restore<K>) -> bool {
        state_repo::finish_restore(&mut self.repo, &mut self.state, restore.0)
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// See [`KeyRepo::clean`] for details.
    ///
    /// [`KeyRepo::clean`]: crate::repo::object::KeyRepo::clean
    pub fn clean(&mut self) -> crate::Result<()> {
        self.repo.clean()
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.repo.clear_instance();
        self.state.clear();
    }

    /// Delete all data in all instances of the repository.
    ///
    /// See [`KeyRepo::clear_repo`] for details.
    ///
    /// [`KeyRepo::clear_repo`]: crate::repo::key::KeyRepo::clear_repo
    pub fn clear_repo(&mut self) {
        self.repo.clear_repo();
        self.state.clear();
    }

    /// Change the password for this repository.
    ///
    /// See [`KeyRepo::change_password`] for details.
    ///
    /// [`KeyRepo::change_password`]: crate::repo::key::KeyRepo::change_password
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
