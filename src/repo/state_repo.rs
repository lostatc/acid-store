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

use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::repo::OpenRepo;

use super::common::{Key, KeyRepo, Restore as KeyRestore, Savepoint};

/// The keys in the backing `KeyRepo` for the objects which hold the repository state.
///
/// Multiple objects are needed to hold the repository state in order to maintain ACID guarantees.
#[derive(Debug, Clone, Copy)]
pub struct StateKeys<K> {
    pub current: K,
    pub previous: K,
    pub temp: K,
}

/// The state which is read from and written to the backing repository.
///
/// The `Default` value of the state must be a value suitable for a new empty repository.
pub trait RepoState: DeserializeOwned + Serialize + Default {
    /// Clear the state, returning it to its `Default` value.
    fn clear(&mut self);
}

/// An in-progress operation to restore a repository to a [`Savepoint`].
///
/// Repository implementations can wrap this type to hide the type parameters.
///
/// [`Savepoint`]: crate::repo::Savepoint
#[derive(Debug, Clone)]
pub struct Restore<K, S> {
    state: S,
    restore: KeyRestore<K>,
}

impl<K, S> Restore<K, S> {
    /// Return whether the savepoint used to start this restore is valid.
    pub fn is_valid(&self) -> bool {
        self.restore.is_valid()
    }

    /// The ID of the repository instance this `Restore` is associated with.
    pub fn instance(&self) -> Uuid {
        self.restore.instance()
    }
}

/// A repository type which is backed by a `KeyRepo`.
///
/// This value is a helper which can be used to implement higher-level repository types which are
/// backed by a `KeyRepo`.
#[derive(Debug)]
pub struct StateRepo<K: Key, S: RepoState> {
    /// The backing repository.
    pub repo: KeyRepo<K>,

    /// The value which encapsulates the state for this repository.
    pub state: S,

    /// The keys of the objects which hold the repository state in the backing repository.
    pub keys: StateKeys<K>,
}

impl<K: Key, S: RepoState> StateRepo<K, S> {
    /// Deserialize and return the repository state from the backing `KeyRepo`.
    fn read_state(&mut self) -> crate::Result<S> {
        let mut object = self.repo.object(&self.keys.current).unwrap();
        object.deserialize()
    }

    /// Write the repository state to the backing `KeyRepo`.
    fn write_state(&mut self) -> crate::Result<()> {
        let mut object = self.repo.insert(self.keys.temp.clone());
        object.serialize(&self.state)?;
        drop(object);

        self.repo.copy(&self.keys.temp, self.keys.current.clone());

        if !self.repo.contains(&self.keys.previous) {
            self.repo
                .copy(&self.keys.current, self.keys.previous.clone());
        }

        Ok(())
    }

    /// Commit changes which have been made to the repository.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Write the current repository state.
        self.write_state()?;

        // Copy the previous repository state to a temporary object so we can restore it if
        // committing the backing repository fails.
        self.repo.copy(&self.keys.previous, self.keys.temp.clone());

        // Overwrite the previous repository state with the current repository state so that if the
        // commit succeeds, future rollbacks will restore to this point.
        self.repo
            .copy(&self.keys.current, self.keys.previous.clone());

        // Attempt to commit changes to the backing repository.
        let result = self.repo.commit();

        // If the commit fails, restore the previous repository state from the temporary
        // object so we can still roll back the changes.
        if result.is_err() {
            self.repo.copy(&self.keys.temp, self.keys.previous.clone());
        }

        result
    }

    /// Roll back all changes made since the last commit.
    pub fn rollback(&mut self) -> crate::Result<()> {
        let previous_state = match self.repo.object(&self.keys.previous) {
            Some(mut object) => match object.deserialize() {
                Err(crate::Error::Deserialize) => return Err(crate::Error::Corrupt),
                Err(error) => return Err(error),
                Ok(value) => value,
            },
            None => S::default(),
        };

        self.repo.rollback()?;

        self.state = previous_state;

        Ok(())
    }

    /// Create a new `Savepoint` representing the current state of the repository.
    pub fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.write_state()?;
        self.repo.savepoint()
    }

    /// Start the process of restoring the repository to the given `savepoint`.
    pub fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Restore<K, S>> {
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

        Ok(Restore { state, restore })
    }

    /// Finish the process of restoring the repository to a [`Savepoint`].
    pub fn finish_restore(&mut self, restore: Restore<K, S>) -> bool {
        if !self.repo.finish_restore(restore.restore) {
            return false;
        }
        self.state = restore.state;
        true
    }

    /// Delete all data in the current instance of the repository.
    pub fn clear_instance(&mut self) {
        self.repo.clear_instance();
        self.state.clear();
    }

    /// Delete all data in all instances of the repository.
    pub fn clear_repo(&mut self) {
        self.repo.clear_repo();
        self.state.clear();
    }
}

/// A trait which implements `OpenRepo` on a repository backed by a `StateRepo`.
pub trait OpenStateRepo {
    /// The type of the key used in the backing `KeyRepo`.
    type Key: Key;

    /// The type of the value which encapsulates the state for the repository.
    type State: RepoState;

    /// The version ID for the serialized data format of this repository.
    const VERSION_ID: Uuid;

    /// The keys of the objects which hold the repository state in the backing repository.
    const STATE_KEYS: StateKeys<Self::Key>;

    /// Return a new repository of this type backed by the `state_repo`.
    fn from_repo(repo: StateRepo<Self::Key, Self::State>) -> Self;

    /// Consume this repository and return the backing `KeyRepo`.
    fn into_repo(self) -> StateRepo<Self::Key, Self::State>;
}

impl<T: OpenStateRepo> OpenRepo for T {
    type Key = T::Key;

    const VERSION_ID: Uuid = T::VERSION_ID;

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut new_repo = StateRepo {
            repo,
            state: T::State::default(),
            keys: T::STATE_KEYS,
        };
        new_repo.state = new_repo.read_state()?;
        Ok(T::from_repo(new_repo))
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mut new_repo = StateRepo {
            repo,
            state: T::State::default(),
            keys: T::STATE_KEYS,
        };
        new_repo.write_state()?;
        Ok(T::from_repo(new_repo))
    }

    fn into_repo(self) -> crate::Result<KeyRepo<Self::Key>> {
        let mut repo = self.into_repo();
        repo.write_state()?;
        Ok(repo.repo)
    }
}
