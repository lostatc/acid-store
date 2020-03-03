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
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::aead::xchacha20poly1305_ietf::{
    gen_nonce, open, seal, Key as ChaChaKey, Nonce, KEYBYTES, NONCEBYTES,
};
use sodiumoxide::crypto::pwhash::argon2id13::{
    derive_key, gen_salt, MemLimit, OpsLimit, Salt, MEMLIMIT_INTERACTIVE, MEMLIMIT_MODERATE,
    MEMLIMIT_SENSITIVE, OPSLIMIT_INTERACTIVE, OPSLIMIT_MODERATE, OPSLIMIT_SENSITIVE,
};
use zeroize::Zeroize;

/// A limit on the resources used by a key derivation function.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
pub enum ResourceLimit {
    /// Suitable for interactive use.
    Interactive,

    /// Suitable for moderately sensitive data.
    Moderate,

    /// Suitable for highly sensitive data.
    Sensitive,
}

impl ResourceLimit {
    /// Get a memory limit based on this resource limit.
    pub(super) fn to_mem_limit(&self) -> MemLimit {
        match self {
            ResourceLimit::Interactive => MEMLIMIT_INTERACTIVE,
            ResourceLimit::Moderate => MEMLIMIT_MODERATE,
            ResourceLimit::Sensitive => MEMLIMIT_SENSITIVE,
        }
    }

    /// Get an operations limit based on this resource limit.
    pub(super) fn to_ops_limit(&self) -> OpsLimit {
        match self {
            ResourceLimit::Interactive => OPSLIMIT_INTERACTIVE,
            ResourceLimit::Moderate => OPSLIMIT_MODERATE,
            ResourceLimit::Sensitive => OPSLIMIT_SENSITIVE,
        }
    }
}

/// A data encryption method.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Encryption {
    /// Do not encrypt data.
    None,

    /// Encrypt data using the XChaCha20-Poly1305 cipher.
    XChaCha20Poly1305,
}

impl Encryption {
    /// Encrypt the given `cleartext` with the given `key`.
    pub(super) fn encrypt(&self, cleartext: &[u8], key: &EncryptionKey) -> Vec<u8> {
        match self {
            Encryption::None => cleartext.to_vec(),
            Encryption::XChaCha20Poly1305 => {
                let nonce = gen_nonce();
                let chacha_key = ChaChaKey::from_slice(key.0.as_ref()).unwrap();
                let mut ciphertext = seal(&cleartext, None, &nonce, &chacha_key);
                let mut output = nonce.as_ref().to_vec();
                output.append(&mut ciphertext);
                output
            }
        }
    }

    /// Decrypt the given `ciphertext` with the given `key`.
    pub(super) fn decrypt(&self, ciphertext: &[u8], key: &EncryptionKey) -> crate::Result<Vec<u8>> {
        match self {
            Encryption::None => Ok(ciphertext.to_vec()),
            Encryption::XChaCha20Poly1305 => {
                let nonce = Nonce::from_slice(&ciphertext[..NONCEBYTES]).unwrap();
                let chacha_key = ChaChaKey::from_slice(key.0.as_ref()).unwrap();
                open(&ciphertext[NONCEBYTES..], None, &nonce, &chacha_key)
                    .map_err(|_| crate::Error::InvalidData)
            }
        }
    }
}

impl Encryption {
    /// The key size for this encryption method.
    pub(super) fn key_size(&self) -> usize {
        match self {
            Encryption::None => 0,
            Encryption::XChaCha20Poly1305 => KEYBYTES,
        }
    }
}

/// Salt for deriving an encryption `Key`.
///
/// This type can be serialized to persistently store the salt.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeySalt(Salt);

impl KeySalt {
    /// Generate a new random `KeySalt`.
    pub fn generate() -> Self {
        KeySalt(gen_salt())
    }
}

/// An encryption key.
///
/// This type can be serialized to persistently store the key.
///
/// The bytes of the key are zeroed in memory when this value is dropped.
#[derive(Debug, PartialEq, Eq, Clone, Zeroize, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EncryptionKey(Vec<u8>);

impl AsRef<[u8]> for EncryptionKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl EncryptionKey {
    /// Create an encryption key containing the given `bytes`.
    pub fn new(bytes: Vec<u8>) -> Self {
        EncryptionKey(bytes)
    }

    /// Generate a new random encryption key of the given `size`.
    ///
    /// This uses bytes retrieved from the operating system's cryptographically secure random number
    /// generator.
    pub fn generate(size: usize) -> Self {
        let mut bytes = vec![0u8; size];
        OsRng.fill_bytes(&mut bytes);
        EncryptionKey::new(bytes)
    }

    /// Derive a new encryption key of the given `size` from the given `password` and `salt`.
    ///
    /// This uses the Argon2id key derivation function.
    pub fn derive(
        password: &[u8],
        salt: &KeySalt,
        size: usize,
        memory: MemLimit,
        operations: OpsLimit,
    ) -> Self {
        let mut bytes = vec![0u8; size];
        derive_key(&mut bytes, &password, &salt.0, operations, memory)
            .expect("Failed to derive an encryption key.");
        EncryptionKey::new(bytes)
    }
}
