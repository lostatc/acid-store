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

use serde::{Deserialize, Serialize};

use crate::repo::common::{IdTable, UniqueId};
use crate::repo::key::KeyRepo;
use crate::repo::{InstanceId, RepoId, Restore, RestoreSavepoint};

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum RepoKey {
    Object(UniqueId),
    State,
    IdTable,
    Stage,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoState<State> {
    pub state: State,
    pub id_table: IdTable,
}

#[derive(Debug, Clone)]
pub struct StateRestore<State> {
    pub state: RepoState<State>,
    pub restore: <KeyRepo<RepoKey> as RestoreSavepoint>::Restore,
}

impl<State: Clone> Restore for StateRestore<State> {
    fn is_valid(&self) -> bool {
        self.restore.is_valid()
    }

    fn instance(&self) -> InstanceId {
        self.restore.instance()
    }
}

/// An opaque key which can be used to access an object in a [`StateRepo`].
///
/// [`StateRepo`]: crate::repo::state::StateRepo
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct ObjectKey {
    pub(super) repo_id: RepoId,
    pub(super) instance_id: InstanceId,
    pub(super) object_id: UniqueId,
}
