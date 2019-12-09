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

use secstr::SecVec;
use sodiumoxide::crypto::aead::xchacha20poly1305_ietf::{gen_nonce, Key as ChaChaKey, KEYBYTES, Nonce, NONCEBYTES, open, seal};
use sodiumoxide::crypto::pwhash::argon2id13::{derive_key, gen_salt, MEMLIMIT_INTERACTIVE, OPSLIMIT_INTERACTIVE, Salt, SALTBYTES};
use sodiumoxide::randombytes::randombytes_into;

/// A data encryption method.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Encryption {
    /// Do not encrypt data.
    None,

    /// Encrypt data using the XChaCha20-Poly1305 cipher.
    XChaCha20Poly1305
}

impl Encryption {
    /// Encrypt the given `cleartext` with the given `key`.
    pub(super) fn encrypt(&self, cleartext: &[u8], key: Key) -> Vec<u8> {
        match self {
            Encryption::None => cleartext.to_vec(),
            Encryption::XChaCha20Poly1305 => {
                let nonce = get_nonce();
                let chacha_key = ChaChaKey::from_slice(key.0);
                let ciphertext = seal(&cleartext, None, &nonce, &chacha_key);
                let output = nonce.to_vec();
                output.append(ciphertext);
                output
            }
        }
    }

    /// Decrypt the given `ciphertext` with the given `key`.
    pub(super) fn decrypt(&self, ciphertext: &[u8], key: Key) -> Vec<u8> {
        match self {
            Encryption::None => ciphertext.to_vec(),
            Encryption::XChaCha20Poly1305 => {
                let nonce = Nonce::from_slice(&ciphertext[..NONCEBYTES]);
                let chacha_key = ChaChaKey::from_slice(key.0);
                open(&ciphertext[NONCEBYTES..], None, &nonce, &chacha_key)
            }
        }
    }
}

impl Encryption {
    /// The key size for this encryption method.
    pub fn key_size(&self) -> usize {
        match self {
            Encryption::None => 0,
            Encryption::XChaCha20Poly1305 => KEYBYTES
        }
    }
}

/// Salt for deriving an encryption `Key`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySalt(Salt);

impl KeySalt {
    /// Generate a new random `KeySalt`.
    pub fn generate() -> Self {
        KeySalt(gen_salt())
    }
}

/// An encryption key.
///
/// The bytes of the key are zeroed in memory when this value is dropped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Key(SecVec<u8>);

impl Key {
    /// Create an encryption key containing the given `bytes`.
    pub fn new(bytes: Vec<u8>) -> Self {
        Key(SecVec::new(bytes))
    }

    /// Generate a new random encryption key of the given `size`.
    pub fn generate(size: usize) -> Self {
        let mut bytes = vec![0u8; size];
        randombytes_into(&mut bytes);
        Key::new(bytes)
    }

    /// Derive a new encryption key of the given `size` from the given `password` and `salt`.
    ///
    /// This uses the Argon2id key derivation function.
    pub fn derive(password: &[u8], salt: &KeySalt, size: usize) -> Self {
        let mut bytes = vec![0u8; usize];
        let salt = Salt;
        derive_key(&mut bytes, &password, &salt.0, OPSLIMIT_INTERACTIVE, MEMLIMIT_INTERACTIVE);
        Key::new(bytes)
    }
}
