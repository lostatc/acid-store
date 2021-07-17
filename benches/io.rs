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

use std::cell::RefCell;
use std::io::{Read, Write};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use hex_literal::hex;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use uuid::Uuid;

use acid_store::repo::key::KeyRepo;
use acid_store::repo::object::ObjectRepo;
use acid_store::repo::{Chunking, Encryption, OpenMode, OpenOptions, Packing};
use acid_store::store::MemoryConfig;

/// The ID of the managed object to write to for the bench test.
const TEST_OBJECT_UUID: Uuid = Uuid::from_bytes(hex!("375e9f00 d476 4f2c a5db 1aec51c0a240"));

/// The number of bytes to write when a trivial amount of data must be written.
const TRIVIAL_DATA_SIZE: usize = 16;

/// Return a buffer containing `size` random bytes for testing purposes.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

fn create_repo_fixed() -> ObjectRepo {
    OpenOptions::new()
        .chunking(Chunking::fixed())
        .mode(OpenMode::CreateNew)
        .open(&MemoryConfig::new())
        .unwrap()
}

fn create_repo_zpaq() -> ObjectRepo {
    OpenOptions::new()
        .chunking(Chunking::zpaq())
        .mode(OpenMode::CreateNew)
        .open(&MemoryConfig::new())
        .unwrap()
}

fn create_repo_fixed_encryption() -> ObjectRepo {
    OpenOptions::new()
        .chunking(Chunking::fixed())
        .encryption(Encryption::XChaCha20Poly1305)
        .packing(Packing::fixed())
        .password(b"password")
        .mode(OpenMode::CreateNew)
        .open(&MemoryConfig::new())
        .unwrap()
}

fn create_repo_zpaq_encryption() -> ObjectRepo {
    OpenOptions::new()
        .chunking(Chunking::zpaq())
        .encryption(Encryption::XChaCha20Poly1305)
        .packing(Packing::fixed())
        .password(b"password")
        .mode(OpenMode::CreateNew)
        .open(&MemoryConfig::new())
        .unwrap()
}

/// Return a list of repositories and test descriptions.
fn test_configs() -> Vec<(ObjectRepo, String)> {
    let fixed = (
        create_repo_fixed(),
        String::from("Chunking::Fixed, Packing::None, Encryption::None"),
    );

    let zpaq = (
        create_repo_zpaq(),
        String::from("Chunking::Zpaq, Packing::None, Encryption::None"),
    );

    let fixed_packing = (
        create_repo_fixed_encryption(),
        String::from("Chunking::Fixed, Packing::Fixed, Encryption::XChaCha20Poly1305"),
    );

    let zpaq_packing = (
        create_repo_zpaq_encryption(),
        String::from("Chunking::Zpaq, Packing::Fixed, Encryption::XChaCha20Poly1305"),
    );

    vec![fixed, zpaq, fixed_packing, zpaq_packing]
}

pub fn insert_object(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("Insert an object");

    for num_objects in [100, 1_000, 10_000].iter() {
        // Create a new repository.
        let mut repo: KeyRepo<String> = OpenOptions::new()
            .mode(OpenMode::CreateNew)
            .open(&MemoryConfig::new())
            .unwrap();

        // Insert objects and write to them but don't commit them.
        for i in 0..*num_objects {
            let mut object = repo.insert(format!("Uncommitted object {}", i));
            object
                .write_all(random_bytes(TRIVIAL_DATA_SIZE).as_slice())
                .unwrap();
            object.flush().unwrap();
        }

        group.throughput(Throughput::Elements(1));

        // Benchmark inserting a new object.
        group.bench_function(
            format!("with {} uncommitted objects", num_objects),
            |bencher| {
                bencher.iter(|| {
                    repo.insert(String::from("Test"));
                });
            },
        );
    }
}

pub fn insert_object_and_write(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("Insert an object and write to it");

    for num_objects in [100, 1_000, 10_000].iter() {
        // Create a new repository.
        let mut repo: KeyRepo<String> = OpenOptions::new()
            .mode(OpenMode::CreateNew)
            .open(&MemoryConfig::new())
            .unwrap();

        // Insert objects and write to them but don't commit them.
        for i in 0..*num_objects {
            let mut object = repo.insert(format!("Uncommitted object {}", i));
            object
                .write_all(random_bytes(TRIVIAL_DATA_SIZE).as_slice())
                .unwrap();
            object.flush().unwrap();
        }

        group.throughput(Throughput::Elements(1));

        // Benchmark inserting a new object and writing to it.
        group.bench_function(
            format!("with {} uncommitted objects", num_objects),
            |bencher| {
                bencher.iter_batched(
                    || random_bytes(TRIVIAL_DATA_SIZE),
                    |data| {
                        let mut object = repo.insert(String::from("Test"));
                        object.write_all(data.as_slice()).unwrap();
                        object.flush().unwrap();
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

pub fn write_object(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("Write to an object");

    let object_size = bytesize::mib(1u64);

    group.throughput(Throughput::Bytes(object_size));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for (repo, name) in test_configs() {
        group.bench_with_input(
            format!("{}, {}", bytesize::to_string(object_size, true), name),
            &RefCell::new(repo),
            |bencher, repo_cell| {
                bencher.iter_batched(
                    || {
                        let mut repo = repo_cell.borrow_mut();
                        repo.add_managed(TEST_OBJECT_UUID);
                        random_bytes(object_size as usize)
                    },
                    |data| {
                        let mut repo = repo_cell.borrow_mut();
                        let mut object = repo.managed_object_mut(TEST_OBJECT_UUID).unwrap();
                        object.write_all(data.as_slice()).unwrap();
                        object.flush().unwrap();
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
}

pub fn read_object(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("Read from an object");

    let object_size = bytesize::mib(1u64);

    group.throughput(Throughput::Bytes(object_size));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);

    for (repo, name) in test_configs() {
        group.bench_with_input(
            format!("{}, {}", bytesize::to_string(object_size, true), name),
            &RefCell::new(repo),
            |bencher, repo_cell| {
                bencher.iter_batched(
                    || {
                        // Write data to the object.
                        let mut repo = repo_cell.borrow_mut();
                        let mut object = repo.add_managed(TEST_OBJECT_UUID);
                        let data = random_bytes(object_size as usize);
                        object.write_all(data.as_slice()).unwrap();
                        object.flush().unwrap();
                    },
                    |_| {
                        // Read data from the object.
                        let repo = repo_cell.borrow_mut();
                        let mut object = repo.managed_object(TEST_OBJECT_UUID).unwrap();
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
criterion_group!(insert, insert_object, insert_object_and_write);
criterion_main!(throughput);
