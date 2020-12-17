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

use std::io::{Read, Write};

use lazy_static::lazy_static;
use uuid::Uuid;

use crate::repo::object::ObjectRepo;

lazy_static! {
    /// The ID of the managed object which stores the version ID.
    static ref VERSION_OBJECT_ID: Uuid =
        Uuid::parse_str("ca1ff9a4-bffd-11ea-9b7d-bba0dbdf3e01").unwrap();
}

/// Check if the given `repository` matches the given `version_id`.
///
/// This returns `true` if the version ID matches and `false` if a version ID has not yet been
/// written to the repository.
///
/// # Errors
/// - `Error::UnsupportedFormat`: The version ID does not match.
/// - `Error::Corrupt` The repository is corrupt.
/// - `Error::InvalidData`: Ciphertext verification failed.
/// - `Error::Store`: An error occurred with the data store.
/// - `Error::Io`: An I/O error occurred.
pub fn check_version(repository: &mut ObjectRepo, version_id: Uuid) -> crate::Result<bool> {
    match repository.managed_object(*VERSION_OBJECT_ID) {
        Some(mut object) => {
            let mut version_buffer = Vec::new();
            object.read_to_end(&mut version_buffer)?;
            drop(object);

            let version =
                Uuid::from_slice(version_buffer.as_slice()).map_err(|_| crate::Error::Corrupt)?;

            if version == version_id {
                Ok(true)
            } else {
                Err(crate::Error::UnsupportedRepo)
            }
        }
        None => {
            let mut object = repository.add_managed(*VERSION_OBJECT_ID);
            object.write_all(version_id.as_bytes())?;
            Ok(false)
        }
    }
}
