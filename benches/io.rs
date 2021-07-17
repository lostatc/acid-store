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

use std::io::{Read, Seek, SeekFrom, Write};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{Chunking, Encryption, OpenMode, OpenOptions, Packing, RepoConfig};
use acid_store::store::MemoryConfig;
use once_cell::sync::Lazy;

/// The object key to use when performing I/O tests.
const TEST_KEY: &str = "test";

/// The size of the data to read and write to objects.
static OBJECT_SIZE: Lazy<u64> = Lazy::new(|| bytesize::mib(1u64));

/// Return a buffer containing `size` random bytes for testing purposes.
fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

fn open_repo(config: &RepoConfig) -> acid_store::Result<KeyRepo<String>> {
    let mut options = OpenOptions::new();

    options.config(config.clone()).mode(OpenMode::CreateNew);

    if config.encryption != Encryption::None {
        options.password(b"Password");
    }

    options.open(&MemoryConfig::new())
}

pub struct TestSpec {
    pub config: RepoConfig,
    pub description: String,
}

static TEST_SPECS: Lazy<Vec<TestSpec>> = Lazy::new(|| {
    vec![
        TestSpec {
            config: {
                let mut config = RepoConfig::default();
                config.chunking = Chunking::fixed();
                config.packing = Packing::None;
                config.encryption = Encryption::None;
                config
            },
            description: String::from("Chunking::Fixed, Packing::None, Encryption::None"),
        },
        TestSpec {
            config: {
                let mut config = RepoConfig::default();
                config.chunking = Chunking::zpaq();
                config.packing = Packing::None;
                config.encryption = Encryption::None;
                config
            },
            description: String::from("Chunking::Zpaq, Packing::None, Encryption::None"),
        },
        TestSpec {
            config: {
                let mut config = RepoConfig::default();
                config.chunking = Chunking::fixed();
                config.packing = Packing::fixed();
                config.encryption = Encryption::XChaCha20Poly1305;
                config
            },
            description: String::from(
                "Chunking::Fixed, Packing::Fixed, Encryption::XChaCha20Poly1305",
            ),
        },
        TestSpec {
            config: {
                let mut config = RepoConfig::default();
                config.chunking = Chunking::zpaq();
                config.packing = Packing::fixed();
                config.encryption = Encryption::XChaCha20Poly1305;
                config
            },
            description: String::from(
                "Chunking::Zpaq, Packing::Fixed, Encryption::XChaCha20Poly1305",
            ),
        },
    ]
});

pub fn write_object(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("Write to an object");

    group.throughput(Throughput::Bytes(*OBJECT_SIZE));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for TestSpec {
        config,
        description,
    } in &*TEST_SPECS
    {
        group.bench_with_input(
            format!(
                "{}, {}",
                bytesize::to_string(*OBJECT_SIZE, true),
                description
            ),
            &config,
            |bencher, config| {
                bencher.iter_batched(
                    || {
                        let mut repo = open_repo(config).unwrap();
                        repo.insert(String::from(TEST_KEY));
                        (repo, random_bytes(*OBJECT_SIZE as usize))
                    },
                    |(repo, data)| {
                        let mut object = repo.object(TEST_KEY).unwrap();
                        object.write_all(data.as_slice()).unwrap();
                        object.commit().unwrap();
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
}

pub fn read_object(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("Read from an object");

    group.throughput(Throughput::Bytes(*OBJECT_SIZE));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for TestSpec {
        config,
        description,
    } in &*TEST_SPECS
    {
        group.bench_with_input(
            format!(
                "{}, {}",
                bytesize::to_string(*OBJECT_SIZE, true),
                description
            ),
            &config,
            |bencher, config| {
                bencher.iter_batched(
                    || {
                        // Write data to the object.
                        let mut repo = open_repo(config).unwrap();
                        let mut object = repo.insert(String::from(TEST_KEY));
                        let data = random_bytes(*OBJECT_SIZE as usize);
                        object.write_all(data.as_slice()).unwrap();
                        object.commit().unwrap();
                        object.seek(SeekFrom::Start(0)).unwrap();
                        repo
                    },
                    |repo| {
                        // Read data from the object.
                        let mut object = repo.object(TEST_KEY).unwrap();
                        let mut buffer = Vec::new();
                        object.read_to_end(&mut buffer).unwrap();
                        buffer
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
}

criterion_group!(throughput, read_object, write_object);
criterion_main!(throughput);
