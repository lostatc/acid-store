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

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Weak};

use uuid::Uuid;
use weak_table::WeakHashSet;

use super::encryption::{Encryption, EncryptionKey};
use crate::store::{BlockId, BlockKey, BlockType, DataStore};

/// A lock acquired on a resource.
///
/// The lock is released when this value is dropped.
#[derive(Debug)]
pub struct Lock<T>(Arc<T>);

/// A value which keeps track of locks on resources identified by generic IDs.
///
/// This locks resources between threads in a process using weak references.
#[derive(Debug)]
pub struct LockTable<T: Eq + Hash>(WeakHashSet<Weak<T>>);

impl<T: Eq + Hash> LockTable<T> {
    /// Create a new empty `LockTable`.
    pub fn new() -> Self {
        Self(WeakHashSet::new())
    }

    /// Attempt to acquire a lock on the given `id`.
    ///
    /// This returns a new lock or `None` if the resource is already locked.
    pub fn acquire_lock(&mut self, id: T) -> Option<Lock<T>> {
        if self.0.contains(&id) {
            None
        } else {
            let id_arc = Arc::from(id);
            self.0.insert(Arc::clone(&id_arc));
            Some(Lock(id_arc))
        }
    }
}

/// A repository which can be unlocked.
pub trait Unlock {
    /// Release this repository's lock.
    ///
    /// This releases this repository's lock on the data store. Typically, the lock is automatically
    /// released when the repository is dropped. However, this method can be used to handle any
    /// errors that occur when releasing the lock and potentially implement retry logic.
    ///
    /// Once this method returns `Ok`, you **must** drop the repository, as as concurrent access to
    /// a repository can cause data loss.
    ///
    /// # Errors
    /// - `Error::Store`: An error occurred with the data store.
    fn unlock(&self) -> crate::Result<()>;

    /// Return whether this repository is currently locked.
    ///
    /// This returns `true` if this repository currently holds a lock on the data store or `false`
    /// if its lock has been released. A lock can be released via [`unlock`] or via a lock handler
    /// set with [`OpenOptions::locking`].
    ///
    /// # Errors
    /// - `Error::Store`: An error occurred with the data store.
    ///
    /// [`unlock`]: crate::repo::Unlock::unlock
    /// [`OpenOptions::locking`]: crate::repo::OpenOptions::locking
    fn is_locked(&self) -> crate::Result<bool>;

    /// Get the current context value of this repository's lock.
    ///
    /// This method returns the context value associated with this repository's lock on the data
    /// store. This is the same context value which is supplied to [`OpenOptions::locking`].
    ///
    /// # Errors
    /// - `Error::NotLocked`: This repository is not locked.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    ///
    /// [`OpenOptions::locking`]: crate::repo::OpenOptions::locking
    fn context(&self) -> crate::Result<Vec<u8>>;

    /// Update the context value of this repository's lock.
    ///
    /// This method changes the context value associated with this repository's lock on the data
    /// store. This is the same context value which is supplied to [`OpenOptions::locking`].
    ///
    /// # Errors
    /// - `Error::Store`: An error occurred with the data store.
    ///
    /// [`OpenOptions::locking`]: crate::repo::OpenOptions::locking
    fn update_context(&self, context: &[u8]) -> crate::Result<()>;
}

/// Attempt to acquire a lock on the given `store`.
///
/// This uses a two-phase locking algorithm to avoid race conditions.
///
/// This returns the `BlockId` of the block containing the lock or `None` if a lock could not be
/// acquired.
///
/// # Errors
/// - `Error::Locked`: The repository is locked.
/// - `Error::InvalidData`: Ciphertext verification failed.
/// - `Error::Store`: An error occurred with the data store.
/// - `Error::Io`: An I/O error occurred.
pub fn lock_store<'a>(
    store: &mut impl DataStore,
    encryption: &Encryption,
    key: &EncryptionKey,
    context: &'a [u8],
    handler: impl FnOnce(&[u8]) -> bool + 'a,
) -> crate::Result<BlockId> {
    let current_lock_id = Uuid::new_v4().into();

    // Check for any existing locks on the repository.
    let existing_locks = store
        .list_blocks(BlockType::Lock)
        .map_err(crate::Error::Store)?;

    match *existing_locks.as_slice() {
        // There are no exising locks.
        [] => {}

        // There is exactly one existing lock.
        [existing_lock_id] => {
            let encrypted_existing_lock_context = store
                .read_block(BlockKey::Lock(existing_lock_id))
                .map_err(crate::Error::Store)?
                .ok_or(crate::Error::Locked)?;
            let existing_lock_context =
                encryption.decrypt(&encrypted_existing_lock_context, key)?;

            // Invoke the lock handler with the existing lock's lock ID to see if it should be
            // removed.
            if handler(existing_lock_context.as_slice()) {
                store
                    .remove_block(BlockKey::Lock(existing_lock_id))
                    .map_err(crate::Error::Store)?;
            } else {
                return Err(crate::Error::Locked);
            }
        }

        // There is more than one existing lock. We do not try to resolve this situation.
        _ => {
            return Err(crate::Error::Locked);
        }
    }

    // Acquire a lock on the repository.
    let encrypted_current_lock_context = encryption.encrypt(context, key);
    store
        .write_block(
            BlockKey::Lock(current_lock_id),
            &encrypted_current_lock_context,
        )
        .map_err(crate::Error::Store)?;

    // Check if any new locks have been acquired since we last checked.
    let existing_locks = store
        .list_blocks(BlockType::Lock)
        .map_err(crate::Error::Store)?;

    if existing_locks == [current_lock_id] {
        // No new locks have been acquired, which means our lock is valid.
        Ok(current_lock_id)
    } else {
        // At least one new lock as been acquired. We must remove our lock and return an error to
        // avoid a race condition. It is possible for two clients to compete for a lock and for
        // neither to acquire one. This locking algorithm does not guarantee that a lock will be
        // granted.
        store
            .remove_block(BlockKey::Lock(current_lock_id))
            .map_err(crate::Error::Store)?;
        Err(crate::Error::Locked)
    }
}

/// Attempt to release a lock on the given `store`.
///
/// This attempts to release the lock with the given lock `id`.
///
/// # Errors
/// - `Error::Store`: An error occurred with the data store.
pub fn unlock_store(store: &mut impl DataStore, id: BlockId) -> crate::Result<()> {
    store
        .remove_block(BlockKey::Lock(id))
        .map_err(crate::Error::Store)?;
    Ok(())
}
