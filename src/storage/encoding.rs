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

use std::io::{self, Cursor, Read};

use cryptostream::read::{Decryptor, Encryptor};
use flate2::Compression as CompressionLevel;
use flate2::read::{GzDecoder, GzEncoder};
use openssl::rand::rand_bytes;
use openssl::symm::Cipher;
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
    /// Wraps the given `reader` to encode its bytes using this compression method.
    pub(super) fn encode<'a>(&self, reader: impl Read + 'a) -> Box<dyn Read + 'a> {
        match self {
            Compression::None => Box::new(reader),
            Compression::Deflate { level } => {
                Box::new(GzEncoder::new(reader, CompressionLevel::new(*level)))
            }
            Compression::Lzma { level } => Box::new(XzEncoder::new(reader, *level)),
        }
    }

    /// Wraps the given `reader` to decode its bytes using this compression method.
    pub(super) fn decode<'a>(&self, reader: impl Read + 'a) -> Box<dyn Read + 'a> {
        match self {
            Compression::None => Box::new(reader),
            Compression::Deflate { .. } => Box::new(GzDecoder::new(reader)),
            Compression::Lzma { .. } => Box::new(XzDecoder::new(reader)),
        }
    }
}

/// The size of a ChaCha20 key in bytes.
const CHACHA20_KEY_SIZE: usize = 32;

/// The size of a ChaCha20 nonce in bytes.
const CHACHA20_NONCE_SIZE: usize = 8;

/// A data encryption method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Encryption {
    /// Do not encrypt data.
    None,

    /// Encrypt data using the ChaCha20-Poly1305 cipher.
    ChaCha20Poly1305 {
        /// The encryption key.
        key: [u8; CHACHA20_KEY_SIZE],
    },
}

impl Encryption {
    /// Wraps the given `reader` to encode its bytes using this encryption method.
    pub(super) fn encode<'a>(&self, reader: impl Read + 'a) -> Box<dyn Read + 'a> {
        match self {
            Encryption::None => Box::new(reader),
            Encryption::ChaCha20Poly1305 { key } => {
                let mut init_vector = [0u8; CHACHA20_NONCE_SIZE];
                rand_bytes(&mut init_vector).expect("Could not generate random nonce.");
                let encryptor = Encryptor::new(
                    reader,
                    Cipher::chacha20_poly1305(),
                    &key.as_ref(),
                    &init_vector,
                )
                .expect("Could not build encrypting reader.");
                Box::new(Cursor::new(init_vector).chain(encryptor))
            }
        }
    }

    /// Wraps the given `reader` to decode its bytes using this encryption method.
    pub(super) fn decode<'a>(&self, mut reader: impl Read + 'a) -> io::Result<Box<dyn Read + 'a>> {
        match self {
            Encryption::None => Ok(Box::new(reader)),
            Encryption::ChaCha20Poly1305 { key } => {
                let mut init_vector = [0u8; CHACHA20_NONCE_SIZE];
                reader.read_exact(&mut init_vector)?;
                let decryptor = Decryptor::new(
                    reader,
                    Cipher::chacha20_poly1305(),
                    &key.as_ref(),
                    &init_vector,
                )
                .expect("Could not build decrypting reader.");
                Ok(Box::new(decryptor))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::io::Read;

    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;

    use super::{CHACHA20_KEY_SIZE, Compression, Encryption};

    /// Return a vector with `size` random bytes.
    fn random_bytes(size: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; size];
        StdRng::from_entropy().fill(&mut bytes[..size]);
        bytes
    }

    /// Fill the given `buffer` with random bytes.
    fn fill_random(buffer: &mut [u8]) {
        StdRng::from_entropy().fill(buffer);
    }

    /// The size of test data to use.
    const TEST_DATA_SIZE: usize = 1024 * 10;

    #[test]
    fn compress_deflate() -> io::Result<()> {
        let expected = random_bytes(TEST_DATA_SIZE);
        let mut actual = Vec::new();
        let compression = Compression::Deflate { level: 2 };
        compression
            .decode(compression.encode(expected.as_slice()))
            .read_to_end(&mut actual)?;

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn compress_lzma() -> io::Result<()> {
        let expected = random_bytes(TEST_DATA_SIZE);
        let mut actual = Vec::new();

        let compression = Compression::Lzma { level: 2 };
        compression
            .decode(compression.encode(expected.as_slice()))
            .read_to_end(&mut actual)?;

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn encrypt_chacha20poly1305() -> io::Result<()> {
        let expected = random_bytes(TEST_DATA_SIZE);
        let mut actual = Vec::new();

        let mut key = [0u8; CHACHA20_KEY_SIZE];
        fill_random(&mut key);

        let encryption = Encryption::ChaCha20Poly1305 { key };
        encryption
            .decode(encryption.encode(expected.as_slice()))?
            .read_to_end(&mut actual)?;

        assert_eq!(expected, actual);
        Ok(())
    }
}
