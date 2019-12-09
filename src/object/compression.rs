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

use std::io::Read;

use flate2::Compression as CompressionLevel;
use flate2::read::{GzDecoder, GzEncoder};
use serde::{Deserialize, Serialize};
use xz2::read::{XzDecoder, XzEncoder};

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

impl Compression {
    /// Wraps the given `reader` to compress its bytes using this compression method.
    pub(super) fn compress<'a>(&self, reader: impl Read + 'a) -> Box<dyn Read + 'a> {
        match self {
            Compression::None => Box::new(reader),
            Compression::Deflate { level } => {
                Box::new(GzEncoder::new(reader, CompressionLevel::new(*level)))
            }
            Compression::Lzma { level } => Box::new(XzEncoder::new(reader, *level)),
        }
    }

    /// Wraps the given `reader` to decompress its bytes using this compression method.
    pub(super) fn decompress<'a>(&self, reader: impl Read + 'a) -> Box<dyn Read + 'a> {
        match self {
            Compression::None => Box::new(reader),
            Compression::Deflate { .. } => Box::new(GzDecoder::new(reader)),
            Compression::Lzma { .. } => Box::new(XzDecoder::new(reader)),
        }
    }
}
