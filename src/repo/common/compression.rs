/*
 * Copyright 2019-2021 Wren Powell
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

#[cfg(feature = "compression")]
use {
    lz4::{Decoder as Lz4Decoder, EncoderBuilder as Lz4EncoderBuilder},
    std::io::{Read, Write},
};

/// A data compression method.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Compression {
    /// Do not compress data.
    None,

    /// Compress data using the LZ4 compression algorithm.
    #[cfg(feature = "compression")]
    #[cfg_attr(docsrs, doc(cfg(feature = "compression")))]
    Lz4 {
        /// The compression level to use.
        ///
        /// This is a number in the range 1-9, where 1 gives the fastest compression and 9 gives the
        /// highest compression ratio.
        level: u32,
    },
}

impl Compression {
    /// Compresses the given `data` and returns it.
    pub(crate) fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            #[cfg(feature = "compression")]
            Compression::Lz4 { level } => {
                let mut output = Vec::with_capacity(data.len());
                let mut encoder = Lz4EncoderBuilder::new().level(*level).build(&mut output)?;
                encoder.write_all(data)?;
                let (_, result) = encoder.finish();
                result?;
                Ok(output)
            }
        }
    }

    /// Wraps the given `reader` to decompress its bytes using this compression method.
    pub(crate) fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        match self {
            Compression::None => Ok(data.to_vec()),
            #[cfg(feature = "compression")]
            Compression::Lz4 { .. } => {
                let mut output = Vec::with_capacity(data.len());
                let mut decoder = Lz4Decoder::new(data)?;
                decoder.read_to_end(&mut output)?;
                let (_, result) = decoder.finish();
                result?;
                Ok(output)
            }
        }
    }
}
