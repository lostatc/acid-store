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

use uuid::Uuid;

use crate::repo::{Key, Object, ReadOnlyObject};
use crate::store::DataStore;

/// Check if the version ID in the given `object` matches the given `version_id`.
///
/// This returns `Ok` if the version ID matches and `Err` otherwise.
pub fn check_version<K: Key, S: DataStore>(
    mut object: ReadOnlyObject<K, S>,
    version_id: Uuid,
) -> crate::Result<()> {
    let mut version_buffer = Vec::new();
    object.read_to_end(&mut version_buffer)?;
    drop(object);

    let version = Uuid::from_slice(version_buffer.as_slice()).map_err(|_| crate::Error::Corrupt)?;
    if version != version_id {
        return Err(crate::Error::UnsupportedFormat);
    }

    Ok(())
}

/// Write the given `version_id` to `object`.
pub fn write_version<K: Key, S: DataStore>(
    mut object: Object<K, S>,
    version_id: Uuid,
) -> crate::Result<()> {
    object.write_all(version_id.as_bytes())?;
    object.flush()?;
    Ok(())
}
