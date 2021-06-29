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

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

use uuid::Uuid;

use super::handle::ObjectHandle;
use super::metadata::Header;

/// A target for rolling back changes in a repository.
///
/// See [`RestoreSavepoint`] for more information.
///
/// [`RestoreSavepoint`]: crate::repo::RestoreSavepoint
#[derive(Debug, Clone)]
pub struct Savepoint {
    /// The header associated with this savepoint.
    ///
    /// This is the header which is used to restore the state of the repository to when this
    /// savepoint was created. This is an `Arc` so that the savepoint can be cloned without cloning
    /// the (potentially large) wrapped `Header`.
    pub(super) header: Arc<Header>,

    /// A weak reference to the ID of the transaction this savepoint is associated with.
    ///
    /// This is used to track when a savepoint has been invalidated. If the transaction ID has been
    /// dropped, that means the savepoint has been invalidated.
    pub(super) transaction_id: Weak<Uuid>,
}

impl Savepoint {
    /// Return whether this savepoint is valid.
    pub fn is_valid(&self) -> bool {
        self.transaction_id.upgrade().is_some()
    }
}

/// An in-progress operation to restore a repository to a [`Savepoint`].
///
/// This value is returned by [`RestoreSavepoint::start_restore`] and can be passed to
/// [`RestoreSavepoint::finish_restore`] to atomically complete the restore.
///
/// Unlike a [`Savepoint`], a `Restore` is associated with a specific instance of a repository.
/// This means it is not possible to start a restore on one instance and complete it on another. To
/// see the ID of the instance this `Restore` is associated with, use [`instance`].
///
/// If this value is dropped, the restore is cancelled.
///
/// [`RestoreSavepoint`]: crate::repo::RestoreSavepoint;
/// [`RestoreSavepoint::start_restore`]: crate::repo::RestoreSavepoint::start_restore
/// [`RestoreSavepoint::finish_restore`]: crate::repo::RestoreSavepoint::finish_restore
/// [`Savepoint`]: crate::repo::Savepoint
/// [`instance`]: crate::repo::Restore::instance
pub trait Restore: Clone {
    /// Return whether the savepoint used to start this restore is valid.
    fn is_valid(&self) -> bool;

    /// The ID of the repository instance this `Restore` is associated with.
    fn instance(&self) -> Uuid;
}

/// A repository which supports restoring to a [`Savepoint`].
///
/// Repositories support creating savepoints and later restoring to those savepoints, undoing any
/// changes made since they were created.
///
/// You can use [`savepoint`] to create a savepoint, and you can use [`start_restore`] and
/// [`finish_restore`] to restore the repository to a savepoint. You can also use [`restore`] to
/// restore the repository in one step instead of two.
///
/// Savepoints aren't just used to "undo" changes; they can also be used to "redo" changes. If you
/// create a savepoint `A` and then later create a savepoint `B`, you can restore to `A` and *then*
/// restore to `B`, even though `B` was created after `A`.
///
/// You can only restore to savepoints created since the last commit; once changes in a repository
/// are committed, all savepoints associated with that repository are invalidated. A savepoint is
/// also invalidated if the repository it is associated with is dropped. You can use
/// [`Savepoint::is_valid`] to determine whether the current savepoint is valid.
///
/// Creating a savepoint does not commit changes to the repository; if the repository is
/// dropped, it will revert to the previous commit and not the most recent savepoint.
///
/// Restoring to a savepoint affects all instances of the repository.
///
/// # Examples
/// This example demonstrates restoring from a savepoint to undo a change to the repository.
/// ```
/// # use std::io::Write;
/// # use acid_store::store::MemoryConfig;
/// # use acid_store::repo::{RestoreSavepoint, OpenOptions, OpenMode, key::KeyRepo};
/// #
/// # let mut repo: KeyRepo<String> = OpenOptions::new()
/// #     .mode(OpenMode::CreateNew)
/// #     .open(&MemoryConfig::new())
/// #     .unwrap();
/// // Create a new savepoint.
/// let savepoint = repo.savepoint().unwrap();
///
/// // Write data to the repository.
/// let mut object = repo.insert(String::from("test"));
/// object.write_all(b"Some data").unwrap();
/// object.commit().unwrap();
/// drop(object);
///
/// // Restore to the savepoint.
/// repo.restore(&savepoint).unwrap();
///
/// assert!(!repo.contains("test"));
/// ```
///
/// [`Savepoint`]: crate::repo::Savepoint
/// [`savepoint`]: crate::repo::RestoreSavepoint::savepoint
/// [`start_restore`]: crate::repo::RestoreSavepoint::start_restore
/// [`finish_restore`]: crate::repo::RestoreSavepoint::finish_restore
/// [`restore`]: crate::repo::RestoreSavepoint::restore
/// [`Savepoint::is_valid`]: crate::repo::Savepoint::is_valid
pub trait RestoreSavepoint {
    type Restore: Restore;

    /// Create a new [`Savepoint`] representing the current state of the repository.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`Savepoint`]: crate::repo::Savepoint
    fn savepoint(&mut self) -> crate::Result<Savepoint>;

    /// Start the process of restoring the repository to the given `savepoint`.
    ///
    /// This method does not restore the repository on its own, but it returns a [`Restore`] value
    /// which can be passed to [`finish_restore`] to atomically complete the restore.
    ///
    /// # Errors
    /// - `Error::InvalidSavepoint`: The given savepoint is invalid or not associated with this
    /// repository.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`Restore`]: crate::repo::Restore
    /// [`finish_restore`]: crate::repo::RestoreSavepoint::finish_restore
    /// [`Savepoint`]: crate::repo::Savepoint
    fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Self::Restore>;

    /// Finish the process of restoring the repository to a [`Savepoint`].
    ///
    /// To start the process of restoring the repository to a savepoint, you must first call
    /// [`start_restore`].
    ///
    /// If this method returns `true`, the repository has been restored. If this method returns
    /// `false`, the savepoint which was used to start the restore process is invalid or the given
    /// [`Restore`] is not associated with the current instance of the repository.
    ///
    /// [`Savepoint`]: crate::repo::Savepoint
    /// [`start_restore`]: crate::repo::RestoreSavepoint::start_restore
    /// [`Restore`]: crate::repo::Restore
    fn finish_restore(&mut self, restore: Self::Restore) -> bool;

    /// Restore the repository to the given `savepoint`.
    ///
    /// This is a convenience method which calls both [`start_restore`] and [`finish_restore`].
    ///
    /// # Errors
    /// - `Error::InvalidSavepoint`: The given savepoint is invalid or not associated with this
    /// repository.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`start_restore`]: crate::repo::RestoreSavepoint::start_restore
    /// [`finish_restore`]: crate::repo::RestoreSavepoint::finish_restore
    fn restore(&mut self, savepoint: &Savepoint) -> crate::Result<()> {
        let restore = self.start_restore(savepoint)?;
        self.finish_restore(restore);
        Ok(())
    }
}

/// A [`Restore`] for a [`KeyRepo`]
#[derive(Debug, Clone)]
pub struct KeyRestore<K> {
    pub(super) objects: HashMap<K, Arc<RwLock<ObjectHandle>>>,
    pub(super) header: Header,
    pub(super) transaction_id: Weak<Uuid>,
    // We need to store the instance ID because it should not be possible to complete this restore
    // if the user switches instances. This value contains the object map for the current instance
    // only, so switching instances should invalidate it.
    pub(super) instance_id: Uuid,
}

impl<K: Clone> Restore for KeyRestore<K> {
    /// Return whether the savepoint used to start this restore is valid.
    fn is_valid(&self) -> bool {
        self.transaction_id.upgrade().is_some()
    }

    /// The ID of the repository instance this `Restore` is associated with.
    fn instance(&self) -> Uuid {
        self.instance_id
    }
}
