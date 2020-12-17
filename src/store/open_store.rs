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

/// A value which can be used to open a `DataStore`.
pub trait OpenStore {
    /// The type of `DataStore` which this value can be used to open.
    type Store: DataStore + 'static;

    /// Open or create a data store of type `Store`.
    ///
    /// This opens the data store, creating it if it does not already exist.
    ///
    /// # Errors
    /// - `Error::UnsupportedStore`: The data store is an unsupported format. This can happen if
    /// the serialized data format changed or if the storage represented by this value does not
    /// contain a valid data store.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    fn open(&self) -> crate::Result<Self::Store>;
}
