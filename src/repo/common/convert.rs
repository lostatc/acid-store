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

use super::repository::ObjectRepo;

/// A repository which is backed by an `ObjectRepo`.
///
/// Repository types which implement this trait can be opened or created using `OpenOptions`.
pub trait ConvertRepo {
    /// Convert the given `repository` to a repository of this type.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The backing repository is an unsupported format. This can
    /// happen if the serialized data format changed or if the backing repository already contains a
    /// different type of repository.
    /// - `Error::Deserialize`: Could not deserialize data in the repository.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    fn from_repo(repository: ObjectRepo) -> crate::Result<Self>
    where
        Self: Sized;

    /// Consume this repository and return the backing `ObjectRepo`.
    ///
    /// This rolls back any uncommitted changes before returning.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    fn into_repo(self) -> crate::Result<ObjectRepo>;

    /// Switch from one instance of a repository to another.
    ///
    /// This method consumes this repository and returns a new repository of type `R`. This accepts
    /// the `id` of the new instance, which is the same instance ID you would provide to
    /// `OpenOptions::instance`.
    ///
    /// This rolls back any uncommitted changes before returning.
    ///
    /// See the module-level documentation for `acid_store::repo` for more information on repository
    /// instances.
    fn switch_instance<R>(self, id: Uuid) -> crate::Result<R>
    where
        R: ConvertRepo,
        Self: Sized,
    {
        let mut repo = self.into_repo()?;
        repo.set_instance(id);
        R::from_repo(repo)
    }
}
