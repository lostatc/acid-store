/*
 * Copyright 2019-2021 Wren Powell
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
use std::convert::TryInto;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;
use std::time::SystemTime;

use hex_literal::hex;
use uuid::Uuid;

use crate::repo::key::KeyRepo;
use crate::repo::state::StateRepo;
use crate::repo::{
    key::Key, Commit, InstanceId, Object, OpenRepo, ReadOnlyObject, RepoInfo, RepoStats,
    ResourceLimit, RestoreSavepoint, Savepoint, VersionId,
};

use super::info::{KeyInfo, Version, VersionInfo};
use super::iter::{Keys, Versions};

/// The state for a `VersionRepo`.
pub type RepoState<K> = HashMap<K, KeyInfo>;

/// The ID of the first version of an object.
const INITIAL_VERSION_ID: u32 = 1;

/// An object store with support for content versioning.
///
/// See [`crate::repo::version`] for more information.
#[derive(Debug)]
pub struct VersionRepo<K: Key>(StateRepo<RepoState<K>>);

impl<K: Key> OpenRepo for VersionRepo<K> {
    type Key = <StateRepo<RepoState<K>> as OpenRepo>::Key;

    const VERSION_ID: VersionId = VersionId::new(Uuid::from_bytes(hex!(
        "41a76832 cfc4 11eb ad05 93c1b714dd17"
    )));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self(StateRepo::open_repo(repo)?))
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self(StateRepo::create_repo(repo)?))
    }

    fn into_repo(self) -> crate::Result<KeyRepo<Self::Key>> {
        self.0.into_repo()
    }
}

impl<K: Key> VersionRepo<K> {
    /// Return whether the given `key` exists in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.0.state().contains_key(key)
    }

    /// Insert the given `key` into the repository and return a new object.
    ///
    /// The returned object represents the current version of the key. If the given key already
    /// exists in the repository, this returns `None`.
    pub fn insert(&mut self, key: K) -> Option<Object> {
        if self.0.state().contains_key(&key) {
            return None;
        }

        let object_id = self.0.create();
        let key_info = KeyInfo {
            versions: BTreeMap::new(),
            object: object_id,
        };

        self.0.state_mut().insert(key, key_info);
        self.0.object(object_id)
    }

    /// Remove the given `key` and all its versions from the repository.
    ///
    /// This returns `true` if the key was removed or `false` if it doesn't exist in the repository.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`Commit::clean`] is called.
    ///
    /// [`Commit::clean`]: crate::repo::Commit::clean
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_info = match self.0.state_mut().remove(key) {
            Some(info) => info,
            None => return false,
        };

        for (_, info) in key_info.versions.iter() {
            self.0.remove(info.id);
        }

        self.0.remove(key_info.object);

        true
    }

    /// Return an `Object` for reading the current version of `key`.
    ///
    /// This returns `None` if the key doesn't exist in the repository.
    pub fn object<Q>(&self, key: &Q) -> Option<Object>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let object_id = self.0.state().get(key)?.object;
        Some(self.0.object(object_id).unwrap())
    }

    /// Return an iterator of all the keys in this repository.
    pub fn keys(&self) -> Keys<K> {
        Keys(self.0.state().keys())
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
        let version_id;
        let object_id;

        {
            let key_info = self.0.state().get(key)?;
            version_id = key_info
                .versions
                .keys()
                .rev()
                .next()
                .map_or(INITIAL_VERSION_ID, |id| id + 1);
            object_id = key_info.object;
        }

        let version_object_id = self.0.copy(object_id).unwrap();

        let version_info = VersionInfo {
            created: SystemTime::now(),
            id: version_object_id,
        };

        let version = Version {
            id: version_id,
            created: version_info.created,
            content_id: self
                .0
                .object(version_info.id)
                .unwrap()
                .content_id()
                .unwrap(),
        };

        self.0
            .state_mut()
            .get_mut(key)?
            .versions
            .insert(version_id, version_info);

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
        let key_info = match self.0.state_mut().get_mut(key) {
            Some(info) => info,
            None => return false,
        };
        let version_info = match key_info.versions.remove(&version_id) {
            Some(info) => info,
            None => return false,
        };

        assert!(self.0.remove(version_info.id));

        true
    }

    /// Return a `ReadOnlyObject` for reading the contents of a version.
    ///
    /// This returns `None` if the version doesn't exist in the repository.
    pub fn version_object<Q>(&self, key: &Q, version_id: u32) -> Option<ReadOnlyObject>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_info = self.0.state().get(key)?;
        let version_info = key_info.versions.get(&version_id)?;
        Some(self.0.object(version_info.id).unwrap().try_into().unwrap())
    }

    /// Return the version of `key` with the given `version_id`.
    ///
    /// This returns `None` if the version doesn't exist in the repository.
    pub fn get_version<Q>(&self, key: &Q, version_id: u32) -> Option<Version>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let version_info = self.0.state().get(key)?.versions.get(&version_id)?;
        Some(Version {
            id: version_id,
            created: version_info.created,
            content_id: self
                .0
                .object(version_info.id)
                .unwrap()
                .content_id()
                .unwrap(),
        })
    }

    /// Return an iterator of versions for the given `key`.
    ///
    /// This returns `None` if the key doesn't exist in the repository.
    ///
    /// The versions are sorted by their version ID, which corresponds to the order they were
    /// created in.
    pub fn versions<Q>(&self, key: &Q) -> Option<Versions>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let versions = self.0.state().get(key)?.versions.clone().into_iter();
        Some(Versions {
            versions,
            id_factory: Box::new(move |object_key| {
                self.0.object(object_key).unwrap().content_id().unwrap()
            }),
        })
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
        let version_object_id = {
            let key_info = match self.0.state().get(key) {
                Some(info) => info,
                None => return false,
            };
            match key_info.versions.get(&version_id) {
                Some(info) => info.id,
                None => return false,
            }
        };
        let new_object_id = self.0.copy(version_object_id).unwrap();
        let old_object_id = {
            let key_info = self.0.state_mut().get_mut(key).unwrap();
            mem::replace(&mut key_info.object, new_object_id)
        };
        self.0.remove(old_object_id);

        true
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.0.clear_instance()
    }

    /// Change the password for this repository.
    ///
    /// See [`KeyRepo::change_password`] for details.
    ///
    /// [`KeyRepo::change_password`]: crate::repo::key::KeyRepo::change_password
    pub fn change_password(
        &mut self,
        new_password: &[u8],
        memory_limit: ResourceLimit,
        operations_limit: ResourceLimit,
    ) {
        self.0
            .change_password(new_password, memory_limit, operations_limit);
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> InstanceId {
        self.0.instance()
    }

    /// Compute statistics about the repository.
    ///
    /// See [`KeyRepo::stats`] for details.
    ///
    /// [`KeyRepo::stats`]: crate::repo::key::KeyRepo::stats
    pub fn stats(&self) -> RepoStats {
        self.0.stats()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.0.info()
    }
}

impl<K: Key> Commit for VersionRepo<K> {
    fn commit(&mut self) -> crate::Result<()> {
        self.0.commit()
    }

    fn rollback(&mut self) -> crate::Result<()> {
        self.0.rollback()
    }

    fn clean(&mut self) -> crate::Result<()> {
        self.0.clean()
    }
}

impl<K: Key> RestoreSavepoint for VersionRepo<K> {
    type Restore = <StateRepo<RepoState<K>> as RestoreSavepoint>::Restore;

    fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.0.savepoint()
    }

    fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Self::Restore> {
        self.0.start_restore(savepoint)
    }

    fn finish_restore(&mut self, restore: Self::Restore) -> bool {
        self.0.finish_restore(restore)
    }
}
