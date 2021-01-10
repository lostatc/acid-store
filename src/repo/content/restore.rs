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

use crate::repo::key::Restore as KeyRestore;

use super::state::{ContentRepoKey, ContentRepoState};

/// An in-progress operation to restore a [`ContentRepo`] to a [`Savepoint`].
///
/// See [`Restore`] for details.
///
/// [`ContentRepo`]: crate::repo::content::ContentRepo
/// [`Savepoint`]: crate::repo::Savepoint
/// [`Restore`]: crate::repo::key::Restore
#[derive(Debug, Clone)]
pub struct Restore<'a> {
    pub(super) state: ContentRepoState,
    pub(super) restore: KeyRestore<'a, ContentRepoKey>,
}

impl<'a> Restore<'a> {
    /// Return whether the savepoint used to start this restore is valid.
    pub fn is_valid(&self) -> bool {
        self.restore.is_valid()
    }
}
