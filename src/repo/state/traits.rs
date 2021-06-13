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

use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use crate::repo::Savepoint;

pub trait Restore: Clone {
    /// Return whether the savepoint used to start this restore is valid.
    fn is_valid(&self) -> bool;

    /// The ID of the repository instance this `Restore` is associated with.
    fn instance(&self) -> Uuid;
}

pub trait Commit {
    fn commit(&mut self) -> crate::Result<()>;

    fn rollback(&mut self) -> crate::Result<()>;
}

pub trait RestoreSavepoint {
    type Restore: Restore;

    fn savepoint(&mut self) -> crate::Result<Savepoint>;

    fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Self::Restore>;

    fn finish_restore(&mut self, restore: Self::Restore) -> bool;
}

pub trait ClearInstance {
    fn clear_instance(&mut self);
}

pub trait StateStore {
    type State: Serialize + DeserializeOwned;

    fn write_state(&mut self, state: &Self::State) -> crate::Result<()>;

    fn read_state(&mut self) -> crate::Result<Self::State>;
}

pub struct StateRepo<Repo, State> {
    repo: Repo,
    state: State,
}

impl<Repo, State> Commit for StateRepo<Repo, State>
where
    Repo: Commit + RestoreSavepoint + StateStore<State = State>,
    State: Serialize + DeserializeOwned,
{
    fn commit(&mut self) -> crate::Result<()> {
        self.repo.write_state(&self.state)?;
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
        self.state = match self.repo.read_state() {
            Ok(state) => state,
            Err(error) => {
                // If reading the state fails, we must finish restoring the backup so we can return
                // `Err` and have the repository unchanged.
                self.repo.finish_restore(backup_restore);
                return Err(error);
            }
        };

        Ok(())
    }
}

#[derive(Clone)]
struct StateRestore<S: Clone, R: Restore> {
    state: S,
    restore: R,
}

impl<S: Clone, R: Restore> Restore for StateRestore<S, R> {
    fn is_valid(&self) -> bool {
        self.restore.is_valid()
    }

    fn instance(&self) -> Uuid {
        self.restore.instance()
    }
}

impl<Repo, State> RestoreSavepoint for StateRepo<Repo, State>
where
    Repo: RestoreSavepoint + StateStore<State = State>,
    State: Clone + Serialize + DeserializeOwned,
{
    type Restore = StateRestore<State, Repo::Restore>;

    fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.repo.write_state(&self.state)?;
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
        let state = match self.repo.read_state() {
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
        self.state = restore.state;
        true
    }
}
