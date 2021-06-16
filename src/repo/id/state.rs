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

use super::table::{IdTable, ObjectId};
use crate::repo::key::KeyRepo;
use crate::repo::Restore;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Serialize, Deserialize)]
pub enum RepoKey {
    Object(ObjectId),
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
pub struct IdRestore<State> {
    pub state: RepoState<State>,
    pub restore: KeyRepo::Restore,
}

impl<State: Clone> Restore for IdRestore<State> {
    fn is_valid(&self) -> bool {
        self.restore.is_valid()
    }

    fn instance(&self) -> Uuid {
        self.restore.instance()
    }
}
