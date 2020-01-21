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

use std::io::{Read, Seek, SeekFrom, Write};

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};

use data_store::repo::{
    Compression, Encryption, ObjectRepository, RepositoryConfig, ResourceLimit,
};
use data_store::store::MemoryStore;

/// The minimum size of test data buffers.
const MIN_BUFFER_SIZE: usize = 1024;

/// The maximum size of test data buffers.
const MAX_BUFFER_SIZE: usize = 2048;

/// The password to use for testing encrypted repositories.
const PASSWORD: &[u8] = b"password";

/// The archive config to use for testing.
const ARCHIVE_CONFIG: RepositoryConfig = RepositoryConfig {
    chunker_bits: 8,
    encryption: Encryption::XChaCha20Poly1305,
    compression: Compression::Lz4 { level: 2 },
    operations_limit: ResourceLimit::Interactive,
    memory_limit: ResourceLimit::Interactive,
};

/// Return a buffer containing `size` random bytes for testing purposes.
fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

/// Generate a random buffer of bytes of a random size.
fn random_buffer() -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    random_bytes(rng.gen_range(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE))
}

/// Return a new `ObjectRepository` that stores data in memory.
fn new_repository() -> anyhow::Result<ObjectRepository<String, MemoryStore>> {
    Ok(ObjectRepository::create_repo(
        MemoryStore::open(),
        ARCHIVE_CONFIG,
        Some(PASSWORD),
    )?)
}

#[test]
fn read_written_data() -> anyhow::Result<()> {
    let mut repository = new_repository()?;
    let mut object = repository.insert("Test".into());

    let expected_data = random_buffer();
    let mut actual_data = vec![0u8; expected_data.len()];
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    object.seek(SeekFrom::Start(0))?;
    object.read_exact(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test]
fn overwrite_written_data() -> anyhow::Result<()> {
    let mut repository = new_repository()?;
    let mut object = repository.insert("Test".into());

    // Write initial data to the object.
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    object.seek(SeekFrom::Start(0))?;

    // Overwrite that initial data with new data.
    let expected_data = random_buffer();
    let mut actual_data = vec![0u8; expected_data.len()];
    object.write_all(expected_data.as_slice())?;
    object.flush()?;
    object.seek(SeekFrom::Start(0))?;

    // Read the new data..
    object.read_exact(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test]
fn partially_overwrite_written_data() -> anyhow::Result<()> {
    let mut repository = new_repository()?;
    let mut object = repository.insert("Test".into());

    // Write initial data to the object.
    let initial_data = random_buffer();
    object.write_all(initial_data.as_slice())?;
    object.flush()?;
    object.seek(SeekFrom::Start(0))?;

    // Partially overwrite that initial data with new data.
    let new_data = random_bytes(MIN_BUFFER_SIZE / 2);
    object.write_all(new_data.as_slice())?;
    object.flush()?;
    object.seek(SeekFrom::Start(0))?;

    // Read all the data.
    let mut expected_data = initial_data;
    expected_data[..new_data.len()].copy_from_slice(new_data.as_slice());
    let mut actual_data = vec![0u8; expected_data.len()];
    object.read_exact(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}
