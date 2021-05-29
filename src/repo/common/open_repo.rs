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

use uuid::Uuid;

use super::key::Key;
use super::repository::KeyRepo;

/// A repository which can be opened using [`OpenOptions`].
///
/// This trait represents a repository type which can be converted to and from a [`KeyRepo`].
/// This trait can be implemented by repository types so that they can be opened using
/// [`OpenOptions`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
/// [`OpenOptions`]: crate::repo::OpenOptions
pub trait OpenRepo {
    /// The type of the key used in the backing [`KeyRepo`].
    ///
    /// [`KeyRepo`]: crate::repo::key::KeyRepo
    type Key: Key;

    /// The version ID for the serialized data format of this repository.
    ///
    /// This ID is used to distinguish between different repository types and to detect when the
    /// serialized data format of a repository changes. All backwards-incompatible changes to a
    /// repository's serialized data format must change this value.
    const VERSION_ID: Uuid;

    /// Open an existing repository of this type in the backing `repo`.
    ///
    /// **Users of this library should never call this method directly. Use
    /// [`SwitchInstance::switch_instance`] instead.**
    ///
    /// Implementations of this method can safely assume that the given `repo` already contains a
    /// repository of this type.
    ///
    /// This does not commit or roll back changes to the repository.
    ///
    /// # Errors
    /// - `Error::Deserialize`: Could not deserialize data in the repository.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`SwitchInstance::switch_instance`]: crate::repo::SwitchInstance::switch_instance
    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized;

    /// Create a new repository of this type in the backing `repo`.
    ///
    /// **Users of this library should never call this method directly. Use
    /// [`SwitchInstance::switch_instance`] instead.**
    ///
    /// Implementations of this method can safely assume that a repository is not already stored in
    /// the given `repo`.
    ///
    /// This does not commit or roll back changes to the repository.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`SwitchInstance::switch_instance`]: crate::repo::SwitchInstance::switch_instance
    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized;

    /// Consume this repository and return the backing `KeyRepo`.
    ///
    /// **Users of this library should never call this method directly. Use
    /// [`SwitchInstance::switch_instance`] instead.**
    ///
    /// This does not commit or roll back changes to the repository.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`SwitchInstance::switch_instance`]: crate::repo::SwitchInstance::switch_instance
    fn into_repo(self) -> crate::Result<KeyRepo<Self::Key>>;
}

/// A repository which supports switching between instances.
///
/// This trait is automatically implemented for all types which implement [`OpenRepo`].
///
/// [`OpenRepo`]: crate::repo::OpenRepo
pub trait SwitchInstance {
    /// Switch from one instance of this repository to another.
    ///
    /// This method consumes this repository and returns a new repository of type `R`. This accepts
    /// the `id` of the new instance, which is the same instance ID you would provide to
    /// [`OpenOptions::instance`].
    ///
    /// This does not commit or roll back changes to the repository.
    ///
    /// See the module-level documentation for [`crate::repo`] for more information on repository
    /// instances.
    ///
    /// # Examples
    /// ```
    /// use acid_store::uuid::Uuid;
    /// use acid_store::repo::{SwitchInstance, OpenMode, OpenOptions, key::KeyRepo, value::ValueRepo};
    /// use acid_store::store::MemoryConfig;
    ///
    /// let key_instance = Uuid::new_v4();
    /// let value_instance = Uuid::new_v4();
    ///
    /// // Open a repository, specifying an instance ID.
    /// let key_repo: KeyRepo<String> = OpenOptions::new()
    ///     .instance(key_instance)
    ///     .mode(OpenMode::CreateNew)
    ///     .open(&MemoryConfig::new())
    ///     .unwrap();
    ///
    /// // Switch the current instance to an instance of a different type.
    /// let mut value_repo: ValueRepo<u64> = key_repo.switch_instance(value_instance).unwrap();
    ///
    /// // Commit both instances of the repository.
    /// value_repo.commit().unwrap();
    /// ```
    ///
    /// # Errors
    /// - `Error::UnsupportedRepo`: The backing repository is an unsupported format. This can
    /// happen if the serialized data format changed or if the backing repository already contains a
    /// different type of repository.
    /// - `Error::Deserialize`: Could not deserialize data in the repository.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`OpenOptions::instance`]: crate::repo::OpenOptions::instance
    fn switch_instance<R>(self, id: Uuid) -> crate::Result<R>
    where
        R: OpenRepo,
        Self: Sized;
}

impl<T: OpenRepo> SwitchInstance for T {
    fn switch_instance<R>(self, id: Uuid) -> crate::Result<R>
    where
        R: OpenRepo,
        Self: Sized,
    {
        let mut repo = self.into_repo()?;
        repo.write_object_map()?;
        repo.set_instance(id)
    }
}
