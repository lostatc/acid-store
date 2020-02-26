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

use std::fs::{create_dir_all, File, OpenOptions};
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use dirs::{data_dir, runtime_dir};
use fs2::FileExt;
use uuid::Uuid;
use weak_table::WeakHashSet;

use lazy_static::lazy_static;

lazy_static! {
    /// The path of the directory where repository lock files are stored.
    static ref LOCKS_DIR: PathBuf = runtime_dir()
        .unwrap_or_else(|| data_dir().expect("Unsupported platform"))
        .join("acid-store")
        .join("locks");

}

/// A strategy for handling a locked resource.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
pub enum LockStrategy {
    /// Return immediately with an `Err` if the resource is locked.
    Abort,

    /// Block and wait for the lock on the resource to be released.
    ///
    /// This will only block if the lock is held by another process. If the lock is held by another
    /// thread within the same process, this will behave like `LockStrategy::Abort`.
    Wait,

    /// Open the resource and acquires a lock regardless of whether it is already locked.
    ///
    /// If the resource is already locked by another thread or process, that lock is released.
    Force,
}

/// A lock acquired on a resource.
///
/// The lock is released when this value is dropped.
#[derive(Debug)]
pub struct Lock {
    /// The reference that is held to lock the resource within this process.
    id: Arc<Uuid>,

    /// The file lock that is held to lock the resource between processes.
    file: File,
}

/// A value which keeps track of locks on resources identified by UUIDs.
///
/// This locks resources between processes using OS file locks, and it locks resources within a
/// process using weak references.
#[derive(Debug)]
pub struct LockTable(WeakHashSet<Weak<Uuid>>);

impl LockTable {
    /// Create a new empty `LockTable`.
    pub fn new() -> Self {
        Self(WeakHashSet::new())
    }

    /// Attempt to acquire a lock on the given `id` using a given `strategy`.
    ///
    /// This returns a new lock or returns an `Err` if a lock could not be acquired.
    pub fn acquire_lock(&mut self, id: Uuid, strategy: LockStrategy) -> crate::Result<Lock> {
        // Create the lock file if it doesn't already exist.
        create_dir_all(LOCKS_DIR.as_path())?;
        let mut buffer = Uuid::encode_buffer();
        let file_name = format!("{}.lock", id.to_hyphenated().encode_lower(&mut buffer));
        let lock_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(LOCKS_DIR.join(file_name))?;

        // Check if this repository is already locked within this process.
        if self.0.contains(&id) && strategy != LockStrategy::Force {
            Err(crate::Error::Locked)
        } else {
            match strategy {
                LockStrategy::Abort => lock_file
                    .try_lock_exclusive()
                    .map_err(|_| crate::Error::Locked)?,
                LockStrategy::Wait => lock_file.lock_exclusive()?,
                LockStrategy::Force => {
                    lock_file.unlock()?;
                    lock_file
                        .try_lock_exclusive()
                        .map_err(|_| crate::Error::Locked)?;
                }
            };

            let id_arc = Arc::from(id);
            self.0.insert(Arc::clone(&id_arc));

            Ok(Lock {
                id: id_arc,
                file: lock_file,
            })
        }
    }
}
