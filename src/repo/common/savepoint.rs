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

use super::metadata::Header;

/// A target for rolling back changes in a repository.
///
/// Repositories support creating savepoints and later restoring to those savepoints, undoing any
/// changes made since they were created. You can use [`ObjectRepo::savepoint`] to create a
/// savepoint and [`ObjectRepo::restore`] to restore to a savepoint.
///
/// Savepoints aren't just used to "undo" changes; they can also be used to "redo" changes. If you
/// create a savepoint `A` and then later create a savepoint `B`, you can restore to `A` and *then*
/// restore to `B`, even though `B` was created after `A`.
///
/// You can only restore to savepoints created since the last commit; once changes in a repository
/// are committed, all savepoints associated with that repository are invalidated. A savepoint is
/// also invalidated if the repository it is associated with is dropped. You can use [`is_valid`] to
/// determine whether the current savepoint is valid.
///
/// [`ObjectRepo::savepoint`]: crate::repo::object::ObjectRepo::savepoint
/// [`ObjectRepo::restore`]: crate::repo::object::ObjectRepo::restore
/// [`is_valid`]: crate::repo::Savepoint::is_valid
#[derive(Debug)]
pub struct Savepoint {
    /// The header at the point when this savepoint was created.
    ///
    /// Restoring from this savepoint will replace the repository's current header with this value.
    pub(super) header: Box<Header>,

    /// A weak reference to the ID of the transaction this savepoint is associated with.
    ///
    /// This value is used to invalidate the savepoint once the repository is committed.
    pub(super) transaction_id: Weak<Uuid>,
}

impl Savepoint {
    /// Return whether this savepoint is valid.
    pub fn is_valid(&self) -> bool {
        self.transaction_id.upgrade().is_some()
    }
}
