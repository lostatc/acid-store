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

/// The size of the checksums used for uniquely identifying data.
const CHECKSUM_SIZE: usize = 32;

/// A 256-bit checksum used for uniquely identifying data.
pub type Checksum = [u8; CHECKSUM_SIZE];

/// An object in a repository.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Object {
    /// The original size of the data in bytes.
    size: u64,

    /// The checksums of the chunks which make up the data.
    chunks: Vec<Checksum>
}

impl Object {
    /// The size of the data in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }
}
