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

use super::common::{Key, KeyRepo, Restore as KeyRestore, Savepoint};

/// The keys in the backing `KeyRepo` for the objects which hold the repository state.
///
/// Multiple objects are needed to hold the repository state in order to maintain ACID guarantees.
#[derive(Clone, Copy)]
pub struct StateKeys<K> {
    pub current: K,
    pub previous: K,
    pub temp: K,
}

/// An in-progress operation to restore a repository to a [`Savepoint`].
///
/// Repository implementations can wrap this type to hide the type parameters.
///
/// [`Savepoint`]: crate::repo::Savepoint
#[derive(Debug, Clone)]
pub struct Restore<'a, K, S> {
    state: S,
    restore: KeyRestore<'a, K>,
}

impl<'a, K, S> Restore<'a, K, S> {
    /// Return whether the savepoint used to start this restore is valid.
    pub fn is_valid(&self) -> bool {
        self.restore.is_valid()
    }
}

/// Deserialize and return the repository state from the backing [`KeyRepo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
pub fn read_state<K: Key, S: DeserializeOwned>(
    repo: &mut KeyRepo<K>,
    keys: StateKeys<K>,
) -> crate::Result<S> {
    let mut object = repo.object(&keys.current).unwrap();
    object.deserialize()
}

/// Write the repository state to the backing [`KeyRepo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
pub fn write_state<K: Key, S: Serialize>(
    repo: &mut KeyRepo<K>,
    keys: StateKeys<K>,
    state: &S,
) -> crate::Result<()> {
    let mut object = repo.insert(keys.temp);
    object.serialize(state)?;
    drop(object);

    repo.copy(&keys.temp, keys.current);

    if !repo.contains(&keys.previous) {
        repo.copy(&keys.current, keys.previous);
    }

    Ok(())
}

/// Commit changes which have been made to the repository.
///
/// This is used to implement a method which functions like [`KeyRepo::commit`] on a repository type
/// which is backed by a [`KeyRepo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
/// [`KeyRepo::commit`]: crate::repo::key::KeyRepo::commit
pub fn commit<K: Key, S: Serialize>(
    repo: &mut KeyRepo<K>,
    keys: StateKeys<K>,
    state: &S,
) -> crate::Result<()> {
    // Write the current repository state.
    write_state(repo, keys.clone(), state)?;

    // Copy the previous repository state to a temporary object so we can restore it if
    // committing the backing repository fails.
    repo.copy(&keys.previous, keys.temp.clone());

    // Overwrite the previous repository state with the current repository state so that if the
    // commit succeeds, future rollbacks will restore to this point.
    repo.copy(&keys.current, keys.previous.clone());

    // Attempt to commit changes to the backing repository.
    let result = repo.commit();

    // If the commit fails, restore the previous repository state from the temporary
    // object so we can still roll back the changes.
    if result.is_err() {
        repo.copy(&keys.temp, keys.previous.clone());
    }

    result
}

/// Roll back all changes made since the last commit.
///
/// This is used to implement a method which functions like [`KeyRepo::rollback`] on a repository
/// type which is backed by a [`KeyRepo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
/// [`KeyRepo::rollback`]: crate::repo::key::KeyRepo::rollback
pub fn rollback<K: Key, S: DeserializeOwned>(
    repo: &mut KeyRepo<K>,
    keys: StateKeys<K>,
    state: &mut S,
) -> crate::Result<()> {
    let mut object = repo.object(&keys.previous).ok_or(crate::Error::Corrupt)?;
    let new_state = match object.deserialize() {
        Err(crate::Error::Deserialize) => return Err(crate::Error::Corrupt),
        Err(error) => return Err(error),
        Ok(value) => value,
    };
    drop(object);

    repo.rollback()?;

    *state = new_state;

    Ok(())
}

/// Create a new `Savepoint` representing the current state of the repository.
///
/// This is used to implement a method which functions like [`KeyRepo::savepoint`] on a repository
/// type which is backed by a [`KeyRepo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
/// [`KeyRepo::savepoint`]: crate::repo::key::KeyRepo::savepoint
pub fn savepoint<K: Key, S: Serialize>(
    repo: &mut KeyRepo<K>,
    keys: StateKeys<K>,
    state: &S,
) -> crate::Result<Savepoint> {
    write_state(repo, keys, state)?;
    repo.savepoint()
}

/// Start the process of restoring the repository to the given `savepoint`.
///
/// This is used to implement a method which functions like [`KeyRepo::start_restore`] on a
/// repository type which is backed by a [`KeyRepo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
/// [`KeyRepo::start_restore`]: crate::repo::key::KeyRepo::start_restore
pub fn start_restore<'a, K: Key, S: DeserializeOwned>(
    repo: &'a mut KeyRepo<K>,
    keys: StateKeys<K>,
    savepoint: &Savepoint,
) -> crate::Result<Restore<'a, K, S>> {
    // Create a savepoint on the backing repository that we can restore to to undo any changes
    // we make to the repository in this method. This is necessary to uphold the contract that
    // the repository is unchanged when this method returns. It's important that we start the
    // restore process here so that it can be completed infallibly.
    let backup_restore = repo.start_restore(&repo.savepoint()?)?;

    // Temporarily restore the backing repository to the given `savepoint` so we can read the
    // repository state from when the savepoint was created.
    let restore = repo.start_restore(savepoint)?;

    // Note that we clone the `restore` value so that we can also use it in the returned
    // `Restore` value. This is more efficient than calling `start_restore` twice.
    repo.finish_restore(restore.clone());

    // Read the repository state from the backing repository and then restore it to the state it
    // was in before this method was called.
    let state = match read_state(repo, keys.clone()) {
        Ok(state) => {
            repo.finish_restore(backup_restore);
            state
        }
        Err(error) => {
            repo.finish_restore(backup_restore);
            return Err(error);
        }
    };

    Ok(Restore { state, restore })
}

/// Finish the process of restoring the repository to a [`Savepoint`].
///
/// This is used to implement a method which functions like [`KeyRepo::finish_restore`] on a
/// repository type which is backed by a [`KeyRepo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
/// [`KeyRepo::finish_restore`]: crate::repo::key::KeyRepo::finish_restore
pub fn finish_restore<K: Key, S: DeserializeOwned>(
    repo: &mut KeyRepo<K>,
    state: &mut S,
    restore: Restore<K, S>,
) -> bool {
    if !repo.finish_restore(restore.restore) {
        return false;
    }
    *state = restore.state;
    true
}
