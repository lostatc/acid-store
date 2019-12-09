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

/// A data compression method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Compression {
    /// Do not compress data.
    None,

    /// Compress data using the DEFLATE compression algorithm.
    Deflate {
        /// The compression level to use as a number in the range 0-9.
        level: u32,
    },

    /// Compress data using the LZMA compression algorithm.
    Lzma {
        /// The compression level to use as a number in the range 0-9.
        level: u32,
    },
}

/// A data encryption method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Encryption {
    /// Do not encrypt data.
    None,

    /// Encrypt data using the ChaCha20-Poly1305 cipher.
    ChaCha20Poly1305
}
