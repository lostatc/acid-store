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

use hex_literal::hex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::table::{IdTable, ObjectId};
use crate::repo::{
    key::KeyRepo, Commit, Object, OpenRepo, ReadOnlyObject, RepoInfo, Restore, RestoreSavepoint,
    Savepoint,
};

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
enum RepoKey {
    Object(ObjectId),
    State,
    IdTable,
    Stage,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RepoState<State> {
    pub state: State,
    pub id_table: IdTable,
}

#[derive(Debug, Clone)]
struct StateRestore<State> {
    pub state: RepoState<State>,
    pub restore: KeyRepo::Restore,
}

impl<State: Clone> Restore for StateRestore<State> {
    fn is_valid(&self) -> bool {
        self.restore.is_valid()
    }

    fn instance(&self) -> Uuid {
        self.restore.instance()
    }
}

/// A low-level repository type which can be used to implement higher-level repository types
///
/// See [`crate::repo::state`] for more information.
#[derive(Debug)]
pub struct StateRepo<State>
where
    State: Serialize + DeserializeOwned + Default,
{
    repo: KeyRepo<RepoKey>,
    id_table: IdTable,
    state: State,
}

impl<State> OpenRepo for StateRepo<State>
where
    State: Serialize + DeserializeOwned + Default,
{
    type Key = RepoKey;

    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("bb93f91a ce4a 11eb 9c6b b78939b5b629"));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut state_repo = StateRepo {
            repo,
            id_table: IdTable::default(),
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
            id_table: IdTable::default(),
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
            None => IdTable::default(),
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

    /// Return a reference to the encapsulated state.
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Return a mutable reference to the encapsulated state.
    pub fn state_mut(&mut self) -> &mut State {
        &mut self.state
    }

    /// Return whether there is an object with the given `id` in this repository.
    pub fn contains(&self, id: ObjectId) -> bool {
        self.repo.contains(&RepoKey::Object(id))
    }

    /// Create a new object in the repository and returns its `ObjectId`.
    pub fn create(&mut self) -> ObjectId {
        let id = self.id_table.next();
        self.repo.insert(RepoKey::Object(id));
        id
    }

    /// Remove the object with the given `id` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// [`clean`]: crate::repo::Commit::clean
    pub fn remove(&mut self, id: ObjectId) -> bool {
        if !self.id_table.recycle(id) {
            return false;
        }
        assert!(self.repo.remove(&RepoKey::Object(id)));
        true
    }

    /// Return a `ReadOnlyObject` for reading the object with the given `id`.
    ///
    /// This returns `None` if there is no object with the given `id` in the repository.
    ///
    /// The returned object provides read-only access to the data. To get read-write access, use
    /// [`object_mut`].
    ///
    /// [`object_mut`]: crate::repo::state::StateRepo::object_mut
    pub fn object(&self, id: ObjectId) -> Option<ReadOnlyObject> {
        self.repo.object(&RepoKey::Object(id))
    }

    /// Return an `Object` for reading and writing the object with the given `id`.
    ///
    /// This returns `None` if there is no object with the given `id` in the repository.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// [`object`].
    ///
    /// [`object`]: crate::repo::state::StateRepo::object
    pub fn object_mut(&mut self, id: ObjectId) -> Option<Object> {
        self.repo.object_mut(&RepoKey::Object(id))
    }

    /// Return an iterator over all the IDs of objects in this repository.
    pub fn list<'a>(&'a self) -> impl Iterator<Item = ObjectId> + 'a {
        self.repo.keys().filter_map(|key| match key {
            RepoKey::Object(id) => Some(id),
            _ => None,
        })
    }

    /// Create a copy of the object at `source` and return its `ObjectId`.
    ///
    /// If there was no object at `source`, this returns `None`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the object.
    pub fn copy(&mut self, source: ObjectId) -> Option<ObjectId> {
        if !self.repo.contains(&RepoKey::Object(source)) {
            return None;
        }
        let dest_id = self.id_table.next();
        assert!(self
            .repo
            .copy(&RepoKey::Object(source), RepoKey::Object(dest_id)));
        Some(dest_id)
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// See [`KeyRepo::clear_instance`] for details.
    ///
    /// [`KeyRepo::clear_instance`]: crate::repo::key::KeyRepo::clear_instance
    pub fn clear_instance(&mut self) {
        self.state = State::default();
        self.id_table = IdTable::new();
        self.repo.clear_instance();
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
    State: Serialize + DeserializeOwned + Default,
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
