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

use std::fs::metadata;
use std::io;

use rand::{RngCore, SeedableRng};
use rand::rngs::SmallRng;
use tempfile::tempdir;

use disk_archive::{ArchiveConfig, Compression, Encryption, ObjectArchive};

/// Return a buffer containing `size` random bytes for testing purposes.
fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

/// The size of a small test data buffer.
///
/// This is smaller than the archive block size.
const SMALL_DATA_SIZE: usize = 2048;

/// The size of a large test data buffer.
///
/// This is considerably larger than the archive block size, but not an exact multiple of it.
const LARGE_DATA_SIZE: usize = (1024 * 1024 * 4) + 200;

/// The archive config to use for testing.
const ARCHIVE_CONFIG: ArchiveConfig = ArchiveConfig {
    block_size: 4096,
    chunker_bits: 18,
    encryption: Encryption::None,
    compression: Compression::None
};

// TODO: Use macros to generate similar tests.

#[test]
fn small_object_is_persisted() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data = random_bytes(SMALL_DATA_SIZE);
    let object = archive.write(expected_data.as_slice())?;
    archive.insert("Test".to_string(), object);

    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;
    let object = archive.get(&"Test".to_string()).unwrap();
    let actual_data = archive.read_all(&object)?;

    assert_eq!(expected_data, actual_data);

    Ok(())
}

#[test]
fn large_object_is_persisted() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data = random_bytes(LARGE_DATA_SIZE);
    let object = archive.write(expected_data.as_slice())?;
    archive.insert("Test".to_string(), object);

    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;
    let object = archive.get(&"Test".to_string()).unwrap();
    let actual_data = archive.read_all(&object)?;

    assert_eq!(expected_data, actual_data);

    Ok(())
}

#[test]
fn multiple_objects_are_persisted() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data1 = random_bytes(LARGE_DATA_SIZE);
    let object1 = archive.write(expected_data1.as_slice())?;
    archive.insert("Test1".to_string(), object1);

    let expected_data2 = random_bytes(SMALL_DATA_SIZE);
    let object2 = archive.write(expected_data2.as_slice())?;
    archive.insert("Test2".to_string(), object2);

    let expected_data3 = random_bytes(LARGE_DATA_SIZE);
    let object3 = archive.write(expected_data3.as_slice())?;
    archive.insert("Test3".to_string(), object3);

    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;

    let object1 = archive.get(&"Test1".to_string()).unwrap();
    let actual_data1 = archive.read_all(&object1)?;

    let object2 = archive.get(&"Test2".to_string()).unwrap();
    let actual_data2 = archive.read_all(&object2)?;

    let object3 = archive.get(&"Test3".to_string()).unwrap();
    let actual_data3 = archive.read_all(&object3)?;

    assert_eq!(expected_data1, actual_data1);
    assert_eq!(expected_data2, actual_data2);
    assert_eq!(expected_data3, actual_data3);

    Ok(())
}

#[test]
fn removed_objects_are_overwritten() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data1 = random_bytes(LARGE_DATA_SIZE);
    let object1 = archive.write(expected_data1.as_slice())?;
    archive.insert("Test1".to_string(), object1);

    archive.commit()?;
    archive.remove(&"Test1".to_string());
    archive.commit()?;

    let expected_data2 = random_bytes(LARGE_DATA_SIZE);
    let object2 = archive.write(expected_data2.as_slice())?;
    archive.insert("Test2".to_string(), object2);

    archive.commit()?;
    drop(archive);

    let archive_size = metadata(archive_path)?.len();
    assert!(archive_size < (LARGE_DATA_SIZE * 2) as u64);

    Ok(())
}

#[test]
fn uncommitted_changes_are_not_saved() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), Default::default(), None)?;

    let expected_data = b"This is data.";
    let object = archive.write(&mut expected_data.as_ref())?;

    archive.insert("Test".to_string(), object);
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;

    assert_eq!(archive.get(&"Test".to_string()), None);

    Ok(())
}
