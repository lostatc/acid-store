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

use hex_literal::hex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use super::common::ObjectRepo;

/// The ID of the managed object which stores the current repository state.
const STATE_OBJECT_ID: Uuid = Uuid::from_bytes(hex!("649b5a8c 8da6 4faf 811b 848402e64e8b"));

/// The ID of the managed object which stores the repository state as of the previous commit.
///
/// This is necessary to support rolling back changes atomically.
const PREV_STATE_OBJECT_ID: Uuid = Uuid::from_bytes(hex!("c995cd36 61ee 49f9 ae51 2ae0c706f6ce"));

/// Read the current state from the backing `repo`.
pub fn read_state<S: DeserializeOwned>(repo: &mut ObjectRepo) -> crate::Result<S> {
    let mut object = repo
        .managed_object(STATE_OBJECT_ID)
        .ok_or(crate::Error::Corrupt)?;
    object.deserialize()
}

/// Write the current state to the backing `repo`.
pub fn write_state<S: Serialize>(repo: &mut ObjectRepo, state: &S) -> crate::Result<()> {
    let mut object = repo.add_managed(STATE_OBJECT_ID);
    object.serialize(&state)?;
    drop(object);

    if !repo.contains_managed(PREV_STATE_OBJECT_ID) {
        repo.copy_managed(STATE_OBJECT_ID, PREV_STATE_OBJECT_ID);
    }

    Ok(())
}

/// Commit changes which have been made to the repository.
pub fn commit<S: Serialize>(repo: &mut ObjectRepo, state: &S) -> crate::Result<()> {
    // Serialize and write the repository state to the backing repository.
    let mut object = repo.add_managed(STATE_OBJECT_ID);
    object.serialize(state)?;
    drop(object);

    // Create a savepoint in case the commit fails.
    let savepoint = repo.savepoint();

    // Overwrite the previous repository state with the current repository state so that if the
    // commit succeeds, future rollbacks will restore to this point.
    repo.copy_managed(STATE_OBJECT_ID, PREV_STATE_OBJECT_ID);

    // Attempt to commit changes to the backing repository.
    let commit_result = repo.commit();

    // If the commit fails, restore to the savepoint so that we can still roll back changes.
    if commit_result.is_err() {
        repo.restore(savepoint);
    }

    commit_result
}

/// Roll back all changes made since the last commit.
pub fn rollback<S: DeserializeOwned>(repo: &mut ObjectRepo) -> crate::Result<S> {
    // Read and deserialize the repository state as of the previous commit.
    let mut object = repo
        .managed_object(PREV_STATE_OBJECT_ID)
        .ok_or(crate::Error::Corrupt)?;
    let state = match object.deserialize() {
        Err(crate::Error::Deserialize) => return Err(crate::Error::Corrupt),
        Err(error) => return Err(error),
        Ok(value) => value,
    };
    drop(object);

    // Roll back changes to the backing repository.
    repo.rollback()?;

    Ok(state)
}
