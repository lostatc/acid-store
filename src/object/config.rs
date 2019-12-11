/*
 * Copyright 2019 Wren Powell
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

use super::compression::Compression;
use super::encryption::Encryption;

/// The configuration for an archive.
///
/// This type is used to configure an archive when it is created. Once an archive is created, the
/// config values provided cannot be changed. This type implements `Default` to provide a reasonable
/// default configuration.
pub struct ArchiveConfig {
    /// The block size of the archive in bytes.
    ///
    /// Data in the archive is allocated in blocks of this size. Choosing a smaller value may make
    /// the archive more space-efficient, but will hurt performance.
    ///
    /// The default value is `4096` (4KiB).
    pub block_size: u32,

    /// A value which determines the chunk size for content-defined deduplication.
    ///
    /// Data is deduplicated by splitting it into chunks. If two or more objects have a chunk in
    /// common, it will only be stored once. This value determines the average size of those chunks,
    /// which will be 2^`chunker_bits` bytes. Smaller chunks will generally result in better
    /// deduplication ratios and thus a smaller archive, but may hurt performance.
    ///
    /// The default value is `20` (1MiB average chunk size).
    pub chunker_bits: u32,

    /// The compression method to use in the archive.
    ///
    /// The default value is `Compression::None`.
    pub compression: Compression,

    /// The encryption method to use in the archive.
    ///
    /// The default value is `Encryption::None`.
    pub encryption: Encryption,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        ArchiveConfig {
            block_size: 4096,
            chunker_bits: 20,
            compression: Compression::None,
            encryption: Encryption::None,
        }
    }
}
