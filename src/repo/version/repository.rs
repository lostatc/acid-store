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
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::repo::key::Key;
use crate::repo::object::ObjectRepo;
use crate::repo::state_helpers::{commit, read_state, rollback, write_state};
use crate::repo::version::version::VersionInfo;
use crate::repo::{Object, OpenRepo, ReadOnlyObject, RepoInfo, Savepoint};

use super::version::{KeyInfo, Version};

/// The state for a `VersionRepo`.
#[derive(Debug, Serialize, Deserialize)]
struct VersionRepoState<K: Eq + Hash> {
    key_table: HashMap<K, KeyInfo>,
}

/// An object store with support for content versioning.
///
/// See [`crate::repo::version`] for more information.
#[derive(Debug)]
pub struct VersionRepo<K: Key> {
    repo: ObjectRepo,
    state: VersionRepoState<K>,
}

impl<K: Key> OpenRepo for VersionRepo<K> {
    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("9a09fd31 cd63 4267 a173 f53009956ab9"));

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
        let state = VersionRepoState {
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

impl<K: Key> VersionRepo<K> {
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

        let handles = KeyInfo {
            versions: BTreeMap::new(),
            object: self.repo.add_unmanaged(),
        };

        let object_handle = &mut self.state.key_table.entry(key).or_insert(handles).object;
        Some(self.repo.unmanaged_object_mut(object_handle).unwrap())
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
            self.repo.remove_unmanaged(&info.handle);
        }

        self.repo.remove_unmanaged(&key_info.object);

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
        let handle = &self.state.key_table.get(key)?.object;
        self.repo.unmanaged_object(handle)
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
        let handle = &mut self.state.key_table.get_mut(key)?.object;
        self.repo.unmanaged_object_mut(handle)
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
        let version_info = VersionInfo {
            created: SystemTime::now(),
            handle: self.repo.copy_unmanaged(&key_info.object),
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
        let key_info = match self.state.key_table.get_mut(key) {
            Some(info) => info,
            None => return false,
        };
        let version_info = match key_info.versions.remove(&version_id) {
            Some(info) => info,
            None => return false,
        };

        self.repo.remove_unmanaged(&version_info.handle);

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
        Some(self.repo.unmanaged_object(&version_info.handle).unwrap())
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
            self.state
                .key_table
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
        let key_info = match self.state.key_table.get_mut(key) {
            Some(info) => info,
            None => return false,
        };
        let version_info = match key_info.versions.get(&version_id) {
            Some(info) => info,
            None => return false,
        };

        let new_handle = self.repo.copy_unmanaged(&version_info.handle);
        let old_handle = mem::replace(&mut key_info.object, new_handle);
        self.repo.remove_unmanaged(&old_handle);

        true
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
        for key_info in self.state.key_table.values() {
            self.repo.remove_unmanaged(&key_info.object);
            for version_info in key_info.versions.values() {
                self.repo.remove_unmanaged(&version_info.handle);
            }
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
