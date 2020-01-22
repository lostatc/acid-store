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

use std::fmt::Debug;

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};

use data_store::repo::{
    Compression, Encryption, LockStrategy, ObjectRepository, RepositoryConfig, ResourceLimit,
};
use data_store::store::{DataStore, MemoryStore};

/// The minimum size of test data buffers.
pub const MIN_BUFFER_SIZE: usize = 1024;

/// The maximum size of test data buffers.
pub const MAX_BUFFER_SIZE: usize = 2048;

/// The password to use for testing encrypted repositories.
pub const PASSWORD: &[u8] = b"password";

/// The archive config to use for testing.
pub const ARCHIVE_CONFIG: RepositoryConfig = RepositoryConfig {
    chunker_bits: 8,
    encryption: Encryption::XChaCha20Poly1305,
    compression: Compression::Lz4 { level: 2 },
    operations_limit: ResourceLimit::Interactive,
    memory_limit: ResourceLimit::Interactive,
};

/// Assert that the given `expression` matches the given `pattern`.
#[macro_export]
macro_rules! assert_match {
    ($expression:expr, $pattern:pat) => {
        match $expression {
            $pattern => (),
            value => panic!(
                "Expected: {:?}, Received: {:?}",
                stringify!($pattern),
                value
            ),
        }
    };
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
pub fn create_repo() -> data_store::Result<ObjectRepository<String, MemoryStore>> {
    ObjectRepository::create_repo(MemoryStore::open(), ARCHIVE_CONFIG, Some(PASSWORD))
}
