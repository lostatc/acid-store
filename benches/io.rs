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
use std::fs::{remove_dir_all, File, OpenOptions as FileOpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use hex_literal::hex;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use tempfile::tempdir;
use uuid::Uuid;

use acid_store::repo::key::KeyRepo;
use acid_store::repo::object::ObjectRepo;
use acid_store::repo::{Chunking, Encryption, OpenOptions};
use acid_store::store::DirectoryStore;

const TEST_OBJECT_UUID: Uuid = Uuid::from_bytes(hex!("375e9f00 d476 4f2c a5db 1aec51c0a240"));

/// Return a buffer containing `size` random bytes for testing purposes.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

/// Return a new data store at `directory` for benchmarking.
fn new_store(directory: &Path) -> DirectoryStore {
    let store_path = directory.join("store");
    remove_dir_all(&store_path).ok();
    DirectoryStore::new(store_path).unwrap()
}

/// Return an iterator of repositories and test descriptions.
fn test_configs(directory: &Path) -> Vec<(ObjectRepo<DirectoryStore>, String)> {
    let fixed = {
        let store = new_store(&directory.join("fixed"));
        let repo = OpenOptions::new(store)
            .chunking(Chunking::Fixed {
                size: bytesize::mib(1u64) as usize,
            })
            .create_new()
            .unwrap();
        (repo, String::from("Chunking::Fixed, Encryption::None"))
    };

    let fixed_encryption = {
        let store = new_store(&directory.join("fixed"));
        let repo = OpenOptions::new(store)
            .chunking(Chunking::Fixed {
                size: bytesize::mib(1u64) as usize,
            })
            .encryption(Encryption::XChaCha20Poly1305)
            .password(b"password")
            .create_new()
            .unwrap();
        (
            repo,
            String::from("Chunking::Fixed, Encryption::XChaCha20Poly1305"),
        )
    };

    let zpaq = {
        let store = new_store(&directory.join("fixed"));
        let repo = OpenOptions::new(store)
            .chunking(Chunking::Zpaq { bits: 20 })
            .create_new()
            .unwrap();
        (repo, String::from("Chunking::Zpaq, Encryption::None"))
    };

    let zpaq_encryption = {
        let store = new_store(&directory.join("fixed"));
        let repo = OpenOptions::new(store)
            .chunking(Chunking::Zpaq { bits: 20 })
            .encryption(Encryption::XChaCha20Poly1305)
            .password(b"password")
            .create_new()
            .unwrap();
        (
            repo,
            String::from("Chunking::Zpaq, Encryption::XChaCha20Poly1305"),
        )
    };

    vec![fixed, fixed_encryption, zpaq, zpaq_encryption]
}

/// The number of bytes to write when a trivial amount of data must be written.
const TRIVIAL_DATA_SIZE: usize = 16;

pub fn insert_object(criterion: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();
    let mut group = criterion.benchmark_group("Insert an object");

    for num_objects in [100, 1_000, 10_000].iter() {
        // Create a new repository.
        let mut repo = OpenOptions::new(new_store(tmp_dir.path()))
            .create_new::<KeyRepo<String, _>>()
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
    let tmp_dir = tempdir().unwrap();
    let mut group = criterion.benchmark_group("Insert an object and write to it");

    for num_objects in [100, 1_000, 10_000].iter() {
        // Create a new repository.
        let mut repo = OpenOptions::new(new_store(tmp_dir.path()))
            .create_new::<KeyRepo<String, _>>()
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

pub fn write_baseline(criterion: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();
    let file_path = tmp_dir.as_ref().join("test");

    let mut group = criterion.benchmark_group("Baseline write");

    let object_size = bytesize::mib(1u64);

    group.throughput(Throughput::Bytes(object_size));
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(30));

    group.bench_function(bytesize::to_string(object_size, true), |bencher| {
        bencher.iter_batched(
            || {
                let file = File::create(&file_path).unwrap();
                let data = random_bytes(object_size as usize);
                (file, data)
            },
            |(mut file, data)| {
                file.write_all(data.as_slice()).unwrap();
                file.flush().unwrap();
            },
            BatchSize::PerIteration,
        );
    });
}

pub fn write_object(criterion: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();

    let mut group = criterion.benchmark_group("Write to an object");

    let object_size = bytesize::mib(1u64);

    for (mut repo, name) in test_configs(tmp_dir.path()) {
        group.throughput(Throughput::Bytes(object_size));
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(30));

        let object = repo.add_managed(TEST_OBJECT_UUID);

        group.bench_with_input(
            format!("{}, {}", bytesize::to_string(object_size, true), name),
            &RefCell::new(object),
            |bencher, object_cell| {
                bencher.iter_batched(
                    || {
                        // Truncate the object.
                        let mut object = object_cell.borrow_mut();
                        object.truncate(0).unwrap();
                        object.flush().unwrap();
                        random_bytes(object_size as usize)
                    },
                    |data| {
                        // Write data to the object.
                        let mut object = object_cell.borrow_mut();
                        object.write_all(data.as_slice()).unwrap();
                        object.flush().unwrap();
                    },
                    BatchSize::PerIteration,
                );
            },
        );
    }
}

pub fn read_baseline(criterion: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();
    let file_path = tmp_dir.as_ref().join("file");

    let mut group = criterion.benchmark_group("Baseline read");

    let object_size = bytesize::mib(1u64);

    group.throughput(Throughput::Bytes(object_size));
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(30));

    group.bench_function(bytesize::to_string(object_size, true), |bencher| {
        bencher.iter_batched(
            || {
                let mut file = FileOpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .read(true)
                    .open(&file_path)
                    .unwrap();
                file.write_all(random_bytes(object_size as usize).as_slice())
                    .unwrap();
                file.flush().unwrap();
                file
            },
            |mut file| {
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer).unwrap();
                buffer
            },
            BatchSize::PerIteration,
        );
    });
}

pub fn read_object(criterion: &mut Criterion) {
    let tmp_dir = tempdir().unwrap();

    let mut group = criterion.benchmark_group("Read from an object");

    let object_size = bytesize::mib(1u64);

    for (mut repo, name) in test_configs(tmp_dir.path()) {
        group.throughput(Throughput::Bytes(object_size));
        group.sample_size(50);
        group.measurement_time(Duration::from_secs(30));

        let object = repo.add_managed(TEST_OBJECT_UUID);

        group.bench_with_input(
            format!("{}, {}", bytesize::to_string(object_size, true), name),
            &RefCell::new(object),
            |bencher, object_cell| {
                bencher.iter_batched(
                    || {
                        // Write data to the object.
                        let mut object = object_cell.borrow_mut();
                        object.truncate(0).unwrap();
                        object
                            .write_all(random_bytes(object_size as usize).as_slice())
                            .unwrap();
                        object.flush().unwrap();
                    },
                    |_| {
                        // Read data from the object.
                        let mut object = object_cell.borrow_mut();
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

criterion_group!(baseline, write_baseline, read_baseline);
criterion_group!(throughput, read_object, write_object);
criterion_group!(insert, insert_object, insert_object_and_write);
criterion_main!(baseline, throughput, insert);
