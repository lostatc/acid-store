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

use std::collections::HashSet;

use hex_literal::hex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use static_assertions::assert_impl_all;
use uuid::Uuid;

use super::info::{KeyId, KeyIdTable, ObjectKey, RepoKey, RepoState, StateRestore};
use super::iter::Keys;
use crate::repo::{
    key::KeyRepo, Commit, InstanceId, Object, OpenRepo, RepoInfo, RepoStats, ResourceLimit,
    RestoreSavepoint, Savepoint, VersionId,
};

/// A low-level repository type which can be used to implement higher-level repository types
///
/// See [`crate::repo::state`] for more information.
#[derive(Debug)]
pub struct StateRepo<State>
where
    State: Serialize + DeserializeOwned + Default,
{
    repo: KeyRepo<RepoKey>,
    id_table: KeyIdTable,
    state: State,
}

assert_impl_all!(StateRepo<()>: Send, Sync);

impl<State> OpenRepo for StateRepo<State>
where
    State: Serialize + DeserializeOwned + Default,
{
    type Key = RepoKey;

    const VERSION_ID: VersionId = VersionId::new(Uuid::from_bytes(hex!(
        "bb93f91a ce4a 11eb 9c6b b78939b5b629"
    )));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state_repo = StateRepo {
            repo,
            id_table: KeyIdTable::new(),
            state: State::default(),
        };
        let RepoState { state, id_table } = state_repo.read_state()?;
        state_repo.state = state;
        state_repo.id_table = id_table;
        Ok(state_repo)
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state_repo = StateRepo {
            repo,
            id_table: KeyIdTable::new(),
            state: State::default(),
        };
        state_repo.write_state()?;
        Ok(state_repo)
    }

    fn into_repo(mut self) -> crate::Result<KeyRepo<Self::Key>> {
        self.write_state()?;
        Ok(self.repo)
    }
}

impl<State> StateRepo<State>
where
    State: Serialize + DeserializeOwned + Default,
{
    /// Read the state and ID table from the backing repository.
    fn read_state(&mut self) -> crate::Result<RepoState<State>> {
        let state = match self.repo.object(&RepoKey::State) {
            Some(mut object) => object.deserialize()?,
            None => State::default(),
        };
        let id_table = match self.repo.object(&RepoKey::IdTable) {
            Some(mut object) => object.deserialize()?,
            None => KeyIdTable::new(),
        };
        Ok(RepoState { state, id_table })
    }

    /// Write the state and ID table to the backing repository.
    fn write_state(&mut self) -> crate::Result<()> {
        // We write to a temporary object before copying to the final destination to make the write
        // atomic.
        let mut object = self.repo.insert(RepoKey::Stage);
        object.serialize(&self.state)?;
        drop(object);
        self.repo.copy(&RepoKey::Stage, RepoKey::State);

        let mut object = self.repo.insert(RepoKey::Stage);
        object.serialize(&self.id_table)?;
        drop(object);
        self.repo.copy(&RepoKey::Stage, RepoKey::IdTable);

        Ok(())
    }

    /// Create a new `ObjectKey` for the given `object_id`.
    fn new_id(&self, key_id: KeyId) -> ObjectKey {
        ObjectKey {
            repo_id: self.repo.info().id(),
            instance_id: self.repo.instance(),
            key_id,
        }
    }

    /// Return whether the given key belongs to this repository and instance.
    fn check_key(&self, key: ObjectKey) -> bool {
        key.repo_id == self.repo.info().id() && key.instance_id == self.repo.instance()
    }

    /// Return a reference to the encapsulated state.
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Return a mutable reference to the encapsulated state.
    pub fn state_mut(&mut self) -> &mut State {
        &mut self.state
    }

    /// Return whether there is an object with the given `key` in this repository.
    pub fn contains(&self, key: ObjectKey) -> bool {
        self.check_key(key) && self.repo.contains(&RepoKey::Object(key.key_id))
    }

    /// Create a new object in the repository and returns its `ObjectKey`.
    pub fn create(&mut self) -> ObjectKey {
        let object_id = self.id_table.next();
        self.repo.insert(RepoKey::Object(object_id));
        self.new_id(object_id)
    }

    /// Remove the object with the given `key` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`Commit::clean`] is called.
    ///
    /// [`Commit::clean`]: crate::repo::Commit::clean
    pub fn remove(&mut self, key: ObjectKey) -> bool {
        if !self.check_key(key) {
            return false;
        }

        if !self.id_table.recycle(key.key_id) {
            return false;
        }

        assert!(self.repo.remove(&RepoKey::Object(key.key_id)));

        true
    }

    /// Return an `Object` for reading and writing the object with the given `key`.
    ///
    /// This returns `None` if there is no object with the given `key` in the repository.
    pub fn object(&self, key: ObjectKey) -> Option<Object> {
        if !self.check_key(key) {
            return None;
        }

        self.repo.object(&RepoKey::Object(key.key_id))
    }

    /// Return an iterator over all the keys of objects in this repository.
    pub fn keys(&self) -> Keys {
        Keys {
            repo_id: self.repo.info().id(),
            instance_id: self.repo.instance(),
            inner: self.repo.keys(),
        }
    }

    /// Create a copy of the object at `source` and return its `ObjectKey`.
    ///
    /// If there was no object at `source`, this returns `None`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the object.
    pub fn copy(&mut self, source: ObjectKey) -> Option<ObjectKey> {
        if !self.check_key(source) || !self.repo.contains(&RepoKey::Object(source.key_id)) {
            return None;
        }
        let dest_id = self.id_table.next();

        assert!(self
            .repo
            .copy(&RepoKey::Object(source.key_id), RepoKey::Object(dest_id)));

        Some(self.new_id(dest_id))
    }

    /// Verify the integrity of all the data in the repository.
    ///
    /// This returns the set of keys of objects which are corrupt.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&self) -> crate::Result<HashSet<ObjectKey>> {
        Ok(self
            .repo
            .verify()?
            .iter()
            .filter_map(|key| match key {
                RepoKey::Object(id) => Some(self.new_id(*id)),
                _ => None,
            })
            .collect())
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.state = State::default();
        self.id_table = KeyIdTable::new();
        self.repo.clear_instance();
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
        self.repo
            .change_password(new_password, memory_limit, operations_limit);
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> InstanceId {
        self.repo.instance()
    }

    /// Compute statistics about the repository.
    ///
    /// See [`KeyRepo::stats`] for details.
    ///
    /// [`KeyRepo::stats`]: crate::repo::key::KeyRepo::stats
    pub fn stats(&self) -> RepoStats {
        self.repo.stats()
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.repo.info()
    }
}

impl<State> Commit for StateRepo<State>
where
    State: Serialize + DeserializeOwned + Default,
{
    fn commit(&mut self) -> crate::Result<()> {
        self.write_state()?;
        self.repo.commit()
    }

    fn rollback(&mut self) -> crate::Result<()> {
        // Create a savepoint on the backing repository so that we can undo rolling back the backing
        // repository if necessary. This is necessary to uphold the contract that if this method
        // returns `Err`, the repository is unchanged. It's important that we start the restore
        // process here so that it can be completed infallibly.
        let backup_savepoint = self.repo.savepoint()?;
        let backup_restore = self.repo.start_restore(&backup_savepoint)?;

        // Roll back the backing repository.
        self.repo.rollback()?;

        // Roll back this repository's state to the previous commit.
        match self.read_state() {
            Ok(RepoState { state, id_table }) => {
                self.state = state;
                self.id_table = id_table;
                Ok(())
            }
            Err(error) => {
                // If reading the state fails, we must finish restoring the backup so we can return
                // `Err` and have the repository unchanged.
                self.repo.finish_restore(backup_restore);
                Err(error)
            }
        }
    }

    fn clean(&mut self) -> crate::Result<()> {
        self.repo.clean()
    }
}

impl<State> RestoreSavepoint for StateRepo<State>
where
    State: Serialize + DeserializeOwned + Default + Clone,
{
    type Restore = StateRestore<State>;

    fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.write_state()?;
        self.repo.savepoint()
    }

    fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Self::Restore> {
        // Create a savepoint on the backing repository that we can restore to to undo any changes
        // we make to the repository in this method. This is necessary to uphold the contract that
        // the repository is unchanged when this method returns. It's important that we start the
        // restore process here so that it can be completed infallibly.
        let backup_savepoint = self.repo.savepoint()?;
        let backup_restore = self.repo.start_restore(&backup_savepoint)?;

        // Temporarily restore the backing repository to the given `savepoint` so we can read the
        // repository state from when the savepoint was created.
        let restore = self.repo.start_restore(savepoint)?;

        // Note that we clone the `restore` value so that we can also use it in the returned
        // `Restore` value. This is more efficient than calling `start_restore` twice.
        self.repo.finish_restore(restore.clone());

        // Read the repository state from the backing repository and then restore it to the state it
        // was in before this method was called.
        let state = match self.read_state() {
            Ok(state) => {
                self.repo.finish_restore(backup_restore);
                state
            }
            Err(error) => {
                self.repo.finish_restore(backup_restore);
                return Err(error);
            }
        };

        Ok(StateRestore { state, restore })
    }

    fn finish_restore(&mut self, restore: Self::Restore) -> bool {
        if !self.repo.finish_restore(restore.restore) {
            return false;
        }
        let RepoState { state, id_table } = restore.state;
        self.state = state;
        self.id_table = id_table;
        true
    }
}
