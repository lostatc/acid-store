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

use disk_archive::{Compression, Encryption, HashAlgorithm, Key, ObjectArchive, RepositoryConfig};
use disk_archive::Checksum;

/// Return a buffer containing `size` random bytes for testing purposes.
fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

/// Insert random data into the `archive` with the given `key`.
fn insert_data(key: &str, archive: &mut ObjectArchive<String>) -> io::Result<Vec<u8>> {
    let data = random_bytes(DATA_SIZE);
    archive.write(key.to_string(), data.as_slice())?;
    Ok(data)
}

/// Retrieve the data in the `archive` associated with the given `key`.
fn read_data(key: &str, archive: &ObjectArchive<String>) -> io::Result<Vec<u8>> {
    let object = archive.get(&key.to_string()).unwrap();
    archive.read_all(&object)
}

/// The size of a test data buffer.
///
/// This is considerably larger than the archive block size, but not an exact multiple of it.
const DATA_SIZE: usize = (1024 * 1024 * 4) + 200;

/// The archive config to use for testing.
const ARCHIVE_CONFIG: RepositoryConfig = RepositoryConfig {
    block_size: 4096,
    chunker_bits: 18,
    encryption: Encryption::None,
    compression: Compression::None,
    hash_algorithm: HashAlgorithm::Blake2b512,
};

#[test]
fn object_is_persisted() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data = insert_data("Test", &mut archive)?;

    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;

    let actual_data = read_data("Test", &archive)?;

    assert_eq!(expected_data, actual_data);

    Ok(())
}

#[test]
fn multiple_objects_are_persisted() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data1 = insert_data("Test1", &mut archive)?;
    let expected_data2 = insert_data("Test2", &mut archive)?;
    let expected_data3 = insert_data("Test3", &mut archive)?;

    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;

    let actual_data1 = read_data("Test1", &archive)?;
    let actual_data2 = read_data("Test2", &archive)?;
    let actual_data3 = read_data("Test3", &archive)?;

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

    insert_data("Test1", &mut archive)?;

    archive.commit()?;
    archive.remove(&"Test1".to_string());
    archive.commit()?;

    insert_data("Test2", &mut archive)?;

    archive.commit()?;
    drop(archive);

    let archive_size = metadata(archive_path)?.len();
    assert!(archive_size < (DATA_SIZE * 2) as u64);

    Ok(())
}

#[test]
fn uncommitted_changes_are_not_saved() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), Default::default(), None)?;

    insert_data("Test", &mut archive)?;

    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;

    assert_eq!(archive.get(&"Test".to_string()), None);

    Ok(())
}

#[test]
fn encrypted_objects_is_decoded() -> io::Result<()> {
    let mut config = ARCHIVE_CONFIG;
    config.encryption = Encryption::XChaCha20Poly1305;
    let key = Key::generate(Encryption::XChaCha20Poly1305.key_size());

    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), config, Some(key.clone()))?;

    let expected_data = insert_data("Test", &mut archive)?;

    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), Some(key))?;

    let actual_data = read_data("Test", &archive)?;

    assert_eq!(expected_data, actual_data);

    Ok(())
}

#[test]
fn compressed_object_is_decoded() -> io::Result<()> {
    let mut config = ARCHIVE_CONFIG;
    config.compression = Compression::Lz4 { level: 6 };

    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), config, None)?;

    let expected_data = insert_data("Test", &mut archive)?;

    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;

    let actual_data = read_data("Test", &archive)?;

    assert_eq!(expected_data, actual_data);

    Ok(())
}

#[test]
fn compressed_and_encrypted_object_is_decoded() -> io::Result<()> {
    let mut config = ARCHIVE_CONFIG;
    config.compression = Compression::Lz4 { level: 6 };
    config.encryption = Encryption::XChaCha20Poly1305;
    let key = Key::generate(Encryption::XChaCha20Poly1305.key_size());

    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), config, Some(key.clone()))?;

    let expected_data = insert_data("Test", &mut archive)?;

    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), Some(key))?;

    let actual_data = read_data("Test", &archive)?;

    assert_eq!(expected_data, actual_data);

    Ok(())
}

#[test]
fn serialized_object_is_deserialized() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data = (true, 42u32, "Hello!".to_string());
    archive.serialize("Test".to_string(), &expected_data)?;

    archive.commit()?;
    drop(archive);

    let archive: ObjectArchive<String> = ObjectArchive::open(archive_path.as_path(), None)?;
    let object = archive.get(&"Test".to_string()).unwrap();
    let actual_data = archive.deserialize::<(bool, u32, String)>(&object)?;

    assert_eq!(expected_data, actual_data);

    Ok(())
}

#[test]
fn correct_checksum_is_calculated() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data = b"Trans rights are human rights";
    let object = archive.write("Tautology".to_string(), expected_data.as_ref())?;

    let expected_checksum = Checksum {
        algorithm: HashAlgorithm::Blake2b512,
        digest: vec![
            68, 253, 151, 219, 84, 177, 131, 43, 134, 246, 20, 99, 249, 39, 95, 171, 143, 125, 127,
            16, 23, 46, 55, 197, 230, 114, 58, 207, 111, 210, 215, 42, 219, 49, 240, 211, 226, 148,
            200, 83, 238, 64, 99, 118, 160, 38, 83, 168, 74, 126, 131, 252, 112, 173, 185, 89, 136,
            16, 92, 118, 172, 214, 69, 128,
        ],
    };
    let actual_checksum = object.checksum();

    assert_eq!(*actual_checksum, expected_checksum);

    Ok(())
}

#[test]
fn peeking_uuid_returns_correct_value() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive =
        ObjectArchive::<String>::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_uuid = archive.uuid();
    drop(archive);
    let actual_uuid = ObjectArchive::<String>::peek_uuid(archive_path.as_ref())?;

    assert_eq!(actual_uuid, expected_uuid);

    Ok(())
}

#[test]
fn object_is_saved_in_repacked_archive() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let new_archive_path = temp_dir.path().join("repacked_archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), ARCHIVE_CONFIG, None)?;

    let expected_data = insert_data("Test", &mut archive)?;

    archive.commit()?;
    archive.repack(new_archive_path.as_ref())?;

    let new_archive = ObjectArchive::<String>::open(new_archive_path.as_ref(), None)?;
    let actual_data = read_data("Test", &mut archive)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}
