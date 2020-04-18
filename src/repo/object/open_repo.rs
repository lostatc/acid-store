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

use crate::store::DataStore;

use super::config::RepositoryConfig;
use super::lock::LockStrategy;

/// A repository which can be opened.
pub trait OpenRepo<S: DataStore> {
    /// Open the repository in the given data `store`.
    ///
    /// If encryption is enabled, a `password` must be provided. Otherwise, this argument can be
    /// `None`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no repository in the given `store`.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Locked`: The repository is locked and `LockStrategy::Abort` was used.
    /// - `Error::Password`: The password provided is invalid.
    /// - `Error::KeyType`: The type `K` does not match the data in the repository.
    /// - `Error::UnsupportedFormat`: This repository is an unsupported format. This can happen if
    /// the repository format is no longer supported by the current version of the library or if the
    /// repository being opened is of a different type.
    /// - `Error::Store`: An error occurred with the data store.
    fn open_repo(store: S, strategy: LockStrategy, password: Option<&[u8]>) -> crate::Result<Self>
    where
        Self: Sized;

    /// Create a new repository backed by the given data `store`, failing if one already exists.
    ///
    /// A `config` must be provided to configure the new repository. If encryption is enabled, a
    /// `password` must be provided; otherwise, this argument can be `None`.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: A repository already exists in the given `store`.
    /// - `Error::Password` A password was required but not provided or provided but not required.
    /// - `Error::Store`: An error occurred with the data store.
    fn new_repo(store: S, config: RepositoryConfig, password: Option<&[u8]>) -> crate::Result<Self>
    where
        Self: Sized;

    /// Open the repository in the given data `store` if it exists or create one if it doesn't.
    ///
    /// If encryption is enabled, a `password` must be provided. Otherwise, this argument can be
    /// `None`.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Locked`: The repository is locked and `LockStrategy::Abort` was used.
    /// - `Error::Password`: The password provided is invalid.
    /// - `Error::Password` A password was required but not provided or provided but not required.
    /// - `Error::KeyType`: The type `K` does not match the data in the repository.
    /// - `Error::UnsupportedFormat`: This repository is an unsupported format. This can happen if
    /// the repository format is no longer supported by the current version of the library or if the
    /// repository being opened is of a different type.
    /// - `Error::Store`: An error occurred with the data store.
    fn create_repo(
        store: S,
        config: RepositoryConfig,
        strategy: LockStrategy,
        password: Option<&[u8]>,
    ) -> crate::Result<Self>
    where
        Self: Sized;
}
