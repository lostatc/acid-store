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

#![allow(dead_code)]

use std::fmt::Debug;
use std::hash::Hash;

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};

use acid_store::repo::{Compression, Encryption, ObjectRepository, RepositoryConfig};
use acid_store::store::MemoryStore;
use lazy_static::lazy_static;

/// The minimum size of test data buffers.
pub const MIN_BUFFER_SIZE: usize = 1024;

/// The maximum size of test data buffers.
pub const MAX_BUFFER_SIZE: usize = 2048;

/// The password to use for testing encrypted repositories.
pub const PASSWORD: &[u8] = b"password";

lazy_static! {
    /// The archive config to use for testing.
    pub static ref REPO_CONFIG: RepositoryConfig = {
        let mut config = RepositoryConfig::default();
        config.chunker_bits = 8;
        config.encryption = Encryption::XChaCha20Poly1305;
        config.compression = Compression::Lz4 { level: 2 };
        config
    };
}

/// Assert that two collections contain all the same elements, regardless of order.
pub fn assert_contains_all<T: Hash + Eq + Debug>(
    actual: impl IntoIterator<Item = T>,
    expected: impl IntoIterator<Item = T>,
) {
    assert_eq!(
        actual.into_iter().collect::<std::collections::HashSet<_>>(),
        expected
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
    )
}

/// Return a buffer containing `size` random bytes for testing purposes.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

/// Generate a random buffer of bytes of a random size.
pub fn random_buffer() -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    random_bytes(rng.gen_range(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE))
}

/// Create a new `ObjectRepository` that stores data in memory.
pub fn create_repo() -> acid_store::Result<ObjectRepository<String, MemoryStore>> {
    ObjectRepository::create_repo(MemoryStore::new(), REPO_CONFIG.to_owned(), Some(PASSWORD))
}
