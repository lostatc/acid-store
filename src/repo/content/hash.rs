use std::io::Read;

use blake3::Hasher as Blake3;
use digest::{Digest, FixedOutput, Update, VariableOutput};
use serde::{Deserialize, Serialize};

#[cfg(feature = "hash-algorithms")]
use {
    blake2::{VarBlake2b, VarBlake2s},
    sha2::{Sha224, Sha256, Sha384, Sha512, Sha512Trunc224, Sha512Trunc256},
    sha3::{Sha3_224, Sha3_256, Sha3_384, Sha3_512},
};

/// The default hash algorithm to use for `ContentRepo`.
pub const DEFAULT_ALGORITHM: HashAlgorithm = HashAlgorithm::Blake3;

/// The size of the buffer to use when copying bytes.
///
/// We use a 16KiB buffer because that is the minimum size recommended to make use of SIMD
/// instruction sets with BLAKE3.
pub const BUFFER_SIZE: usize = 1024 * 16;

/// A simple digest which supports variable-size output.
///
/// We need this trait because `digest::Digest` does not support variable-sized output.
pub trait SimpleDigest {
    fn update(&mut self, data: &[u8]);

    fn result(self: Box<Self>) -> Vec<u8>;
}

struct FixedDigest<T: Update + FixedOutput>(T);

impl<T: Update + FixedOutput> SimpleDigest for FixedDigest<T> {
    fn update(&mut self, data: &[u8]) {
        self.0.update(data)
    }

    fn result(self: Box<Self>) -> Vec<u8> {
        self.0.finalize_fixed().to_vec()
    }
}

struct VariableDigest<T: Update + VariableOutput>(T);

impl<T: Update + VariableOutput> SimpleDigest for VariableDigest<T> {
    fn update(&mut self, data: &[u8]) {
        self.0.update(data)
    }

    fn result(self: Box<Self>) -> Vec<u8> {
        self.0.finalize_boxed().to_vec()
    }
}

/// A cryptographic hash algorithm.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
#[non_exhaustive]
pub enum HashAlgorithm {
    /// SHA-224
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha224,

    /// SHA-256
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha256,

    /// SHA-384
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha384,

    /// SHA-512
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha512,

    /// SHA-512/224
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha512Trunc224,

    /// SHA-512/256
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha512Trunc256,

    /// SHA3-224
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha3_224,

    /// SHA3-256
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha3_256,

    /// SHA3-384
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha3_384,

    /// SHA3-512
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Sha3_512,

    /// BLAKE2b
    ///
    /// This accepts a digest size in the range of 1-64 bytes.
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Blake2b(usize),

    /// BLAKE2s
    ///
    /// This accepts a digest size in the range of 1-32 bytes.
    #[cfg(feature = "hash-algorithms")]
    #[cfg_attr(docsrs, doc(cfg(feature = "hash-algorithms")))]
    Blake2s(usize),

    /// BLAKE3
    Blake3,
}

impl HashAlgorithm {
    /// The output size of the hash algorithm in bytes.
    #[cfg(feature = "hash-algorithms")]
    pub fn output_size(&self) -> usize {
        match self {
            HashAlgorithm::Sha224 => Sha224::output_size(),
            HashAlgorithm::Sha256 => Sha256::output_size(),
            HashAlgorithm::Sha384 => Sha384::output_size(),
            HashAlgorithm::Sha512 => Sha512::output_size(),
            HashAlgorithm::Sha512Trunc224 => Sha512Trunc224::output_size(),
            HashAlgorithm::Sha512Trunc256 => Sha512Trunc256::output_size(),
            HashAlgorithm::Sha3_224 => Sha3_224::output_size(),
            HashAlgorithm::Sha3_256 => Sha3_256::output_size(),
            HashAlgorithm::Sha3_384 => Sha3_384::output_size(),
            HashAlgorithm::Sha3_512 => Sha3_512::output_size(),
            HashAlgorithm::Blake2b(size) => *size,
            HashAlgorithm::Blake2s(size) => *size,
            HashAlgorithm::Blake3 => Blake3::output_size(),
        }
    }

    /// The output size of the hash algorithm in bytes.
    #[cfg(not(feature = "hash-algorithms"))]
    pub fn output_size(&self) -> usize {
        Blake3::output_size()
    }

    /// Compute and return the hash of the given `data` using this hash algorithm.
    ///
    /// # Errors
    /// - `Error::Io`: An I/O error occurred.
    pub fn hash(&self, mut data: impl Read) -> crate::Result<Vec<u8>> {
        let mut buffer = [0u8; BUFFER_SIZE];
        let mut digest = self.digest();
        let mut bytes_read;

        loop {
            bytes_read = data.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }
            digest.update(&buffer[..bytes_read]);
        }

        Ok(digest.result())
    }

    #[cfg(feature = "hash-algorithms")]
    pub(super) fn digest(&self) -> Box<dyn SimpleDigest> {
        match self {
            HashAlgorithm::Sha224 => Box::new(FixedDigest(Sha224::default())),
            HashAlgorithm::Sha256 => Box::new(FixedDigest(Sha256::default())),
            HashAlgorithm::Sha384 => Box::new(FixedDigest(Sha384::default())),
            HashAlgorithm::Sha512 => Box::new(FixedDigest(Sha512::default())),
            HashAlgorithm::Sha512Trunc224 => Box::new(FixedDigest(Sha512Trunc224::default())),
            HashAlgorithm::Sha512Trunc256 => Box::new(FixedDigest(Sha512Trunc256::default())),
            HashAlgorithm::Sha3_224 => Box::new(FixedDigest(Sha3_224::default())),
            HashAlgorithm::Sha3_256 => Box::new(FixedDigest(Sha3_256::default())),
            HashAlgorithm::Sha3_384 => Box::new(FixedDigest(Sha3_384::default())),
            HashAlgorithm::Sha3_512 => Box::new(FixedDigest(Sha3_512::default())),
            HashAlgorithm::Blake2b(size) => Box::new(VariableDigest(
                VarBlake2b::new(*size).expect("Invalid digest size for BLAKE2b."),
            )),
            HashAlgorithm::Blake2s(size) => Box::new(VariableDigest(
                VarBlake2s::new(*size).expect("Invalid digest size for BLAKE2s."),
            )),
            HashAlgorithm::Blake3 => Box::new(FixedDigest(Blake3::default())),
        }
    }

    #[cfg(not(feature = "hash-algorithms"))]
    pub(super) fn digest(&self) -> Box<dyn SimpleDigest> {
        Box::new(FixedDigest(Blake3::default()))
    }
}
