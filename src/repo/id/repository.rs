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

use std::mem;
use std::hash::Hash;
use serde::{Serialize, Deserialize};

use super::table::{ObjectId, IdTable};

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum IdRepoKey {
    Object(ObjectId),
    State,
    IdTable,
    Stage,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdRepoState<State> {
    pub state: State,
    pub id_table: IdTable,
}

pub struct IdRepo<State>
where
    State: Serialize + DeserializeOwned + Default,
{
    repo: KeyRepo<IdRepoKey>,
    id_table: IdTable,
    state: State,
}

impl<State> IdRepo<State>
where
    State: Serialize + DeserializeOwned + Default,
{
    // TODO: Document
    fn read_state(&mut self) -> crate::Result<IdRepoState<Self::State>> {
        let state = match self.repo.object(&IdRepoKey::State) {
            Some(mut object) => object.deserialize()?,
            None => Self::State::default(),
        };
        let id_table = match self.repo.object(&IdRepoKey::IdTable) {
            Some(mut object) => object.deserialize()?,
            None => IdTable::default(),
        };
        Ok(IdRepoState { state, id_table })
    }

    // TODO: Document
    fn write_state(&mut self) -> crate::Result<()> {
        let mut object = self.repo.insert(IdRepoKey::Stage);
        object.serialize(&self.state)?;
        drop(object);
        self.repo.copy(IdRepoKey::Stage, IdRepoKey::State);

        let mut object = self.repo.insert(IdRepoKey::Stage);
        object.serialize(&self.id_table)?;
        drop(object);
        self.repo.copy(IdRepoKey::Stage, IdRepoKey::IdTable);

        Ok(())
    }

    pub fn state(&mut self) -> &mut Self::State {
        &mut self.state
    }

    /// Return whether there is an object with the given `id` in this repository.
    pub fn contains(&self, id: ObjectId) -> bool {
        self.repo.contains(IdRepoKey::Object(id))
    }

    /// Create a new object in the repository and returns its `ObjectId`.
    pub fn create(&mut self) -> ObjectId {
        let id = self.id_table.next();
        self.repo.insert(IdRepoKey::Object(id));
        id
    }

    /// Remove the object with the given `id` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// [`clean`]: crate::repo::id::IdRepo::clean
    pub fn remove(&mut self, id: ObjectId) -> bool {
        if !self.id_table.recycle(id) {
            return false;
        }
        assert!(self.repo.remove(IdRepoKey::Object(id)));
        true
    }

    /// Return a `ReadOnlyObject` for reading the object with the given `id`.
    ///
    /// This returns `None` if there is no object with the given `id` in the repository.
    ///
    /// The returned object provides read-only access to the data. To get read-write access, use
    /// [`object_mut`].
    ///
    /// [`object_mut`]: crate::repo::id::IdRepo::object_mut
    pub fn object(&self, id: ObjectId) -> Option<ReadOnlyObject> {
        self.repo.object(IdRepoKey::Object(id))
    }

    /// Return an `Object` for reading and writing the object with the given `id`.
    ///
    /// This returns `None` if there is no object with the given `id` in the repository.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// [`object`].
    ///
    /// [`object`]: crate::repo::id::IdRepo::object
    pub fn object_mut(&self, id: ObjectId) -> Option<Object> {
        self.repo.object_mut(IdRepoKey::Object(id))
    }

    /// Return an iterator over all the IDs of objects in this repository.
    pub fn list<'a>(&'a self) -> impl Iterator<Item = ObjectId> + 'a {
        self.repo.keys().filter_map(|key| match key {
            IdRepoKey::Object(id) => Some(id),
            _ => None,
        })
    }

    /// Create a copy of the object at `source` and return its `ObjectId`.
    ///
    /// If there was no object at `source`, this returns `None`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the object.
    pub fn copy(&mut self, source: ObjectId) -> Option<ObjectId> {
        if !self.repo.contains(&IdRepoKey::Object(source)) {
            return None;
        }
        let dest_id = self.id_table.next();
        assert!(self.repo.copy(&IdRepoKey::Object(source), IdRepoKey::Object(dest_id)));
        Some(dest_id)
    }

    /// Commit changes which have been made to the repository.
    ///
    /// See [`KeyRepo::commit`] for details.
    ///
    /// [`KeyRepo::commit`]: crate::repo::key::KeyRepo::commit
    pub fn commit(&mut self) -> crate::Result<()> {
        self.write_state()?;
        self.repo.commit()
    }
}

