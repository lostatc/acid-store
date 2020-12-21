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

use std::sync::Weak;

use uuid::Uuid;

/// A target for rolling back changes in a repository.
///
/// Repositories support creating savepoints and later restoring to those savepoints, undoing any
/// changes made since they were created. You can use [`ObjectRepo::savepoint`] to create a
/// savepoint and [`ObjectRepo::restore`] to restore to a savepoint.
///
/// Restoring to a savepoint will invalidate any savepoints created after them. If you create a
/// savepoint `A` and then later create a savepoint `B`, restoring to `A` will invalidate `B`.
///
/// You can only restore to savepoints created since the last commit; once changes in a repository
/// are committed, all savepoints associated with that repository are invalidated. A savepoint is
/// also invalidated if the repository it is associated with is dropped. You can use [`is_valid`] to
/// determine whether the current savepoint is valid.
///
/// [`ObjectRepo::savepoint`]: crate::repo::object::ObjectRepo::savepoint
/// [`ObjectRepo::restore`]: crate::repo::object::ObjectRepo::restore
/// [`is_valid`]: crate::repo::Savepoint::is_valid
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// A weak reference to the UUID of this savepoint.
    ///
    /// This ID is a weak reference so that `is_valid` can determine whether the savepoint is valid
    /// without having to call a method on the repository. If the savepoint ID has been dropped,
    /// that means the savepoint has been invalidated.
    pub(super) savepoint_id: Weak<Uuid>,
}

impl Savepoint {
    /// Return whether this savepoint is valid.
    pub fn is_valid(&self) -> bool {
        self.savepoint_id.upgrade().is_some()
    }
}
