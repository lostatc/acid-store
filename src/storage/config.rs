/*
 * Copyright 2019 Garrett Powell
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

use serde::{Deserialize, Serialize};

use super::encoding::{Compression, Encryption};

/// The configuration for an archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ArchiveConfig {
    /// The compression method to use for data in the archive.
    pub compression: Compression,

    /// The encryption method to use for data and metadata in the archive.
    pub encryption: Encryption,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        ArchiveConfig {
            compression: Compression::None,
            encryption: Encryption::None,
        }
    }
}
