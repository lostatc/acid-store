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

#![cfg(all(feature = "encryption", feature = "compression"))]

use std::convert::TryFrom;
use std::io::{Read, Seek, SeekFrom, Write};

use test_case::test_case;

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{
    Chunking, Compression, Encryption, OpenMode, OpenOptions, ReadOnlyObject, RepoConfig,
};
use acid_store::store::MemoryConfig;
use common::{random_buffer, random_bytes, MIN_BUFFER_SIZE};

mod common;

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn read_written_data(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    let expected_data = random_buffer();
    let mut actual_data = vec![0u8; expected_data.len()];
    object.write_all(expected_data.as_slice())?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;
    object.read_exact(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);
    assert_eq!(object.size().unwrap(), expected_data.len() as u64);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn append_to_object(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    let first_data = random_buffer();
    let second_data = random_buffer();

    object.write_all(first_data.as_slice())?;
    object.commit()?;

    object.seek(SeekFrom::End(0))?;

    object.write_all(second_data.as_slice())?;
    object.commit()?;

    let mut expected_data = first_data;
    expected_data.extend(second_data.as_slice());

    let mut actual_data = Vec::new();
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    assert_eq!(object.size().unwrap(), expected_data.len() as u64);
    assert_eq!(&actual_data, &expected_data);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn seek_and_read_data(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    let original_data = random_buffer();
    let mut actual_data = Vec::new();

    object.write_all(original_data.as_slice())?;
    object.commit()?;

    // Seek from start.
    object.seek(SeekFrom::Start(10))?;
    object.read_to_end(&mut actual_data)?;
    assert_eq!(actual_data, &original_data[10..]);
    actual_data.clear();

    // Seek from end.
    object.seek(SeekFrom::End(10))?;
    object.read_to_end(&mut actual_data)?;
    let start_position = original_data.len() - 10;
    assert_eq!(actual_data, &original_data[start_position..]);
    actual_data.clear();

    // Seek from current position.
    object.seek(SeekFrom::Start(10))?;
    object.seek(SeekFrom::Current(10))?;
    object.read_to_end(&mut actual_data)?;
    assert_eq!(actual_data, &original_data[20..]);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn seek_to_negative_offset(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    // Write initial data to the object.
    object.write_all(random_buffer().as_slice())?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    assert!(object.seek(SeekFrom::Current(-1)).is_err());
    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn overwrite_written_data(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    // Write initial data to the object.
    object.write_all(random_buffer().as_slice())?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Overwrite that initial data with new data.
    let expected_data = random_buffer();
    let mut actual_data = vec![0u8; expected_data.len()];
    object.write_all(expected_data.as_slice())?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Read the new data..
    object.read_exact(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn partially_overwrite_written_data(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    // Write initial data to the object.
    let initial_data = random_buffer();
    object.write_all(initial_data.as_slice())?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Partially overwrite that initial data with new data.
    let new_data = random_bytes(MIN_BUFFER_SIZE / 2);
    object.write_all(new_data.as_slice())?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Read all the data.
    let mut expected_data = initial_data;
    expected_data[..new_data.len()].copy_from_slice(new_data.as_slice());
    let mut actual_data = vec![0u8; expected_data.len()];
    object.read_exact(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn partially_overwrite_and_grow_data(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    let new_start_position = MIN_BUFFER_SIZE / 2;

    // Write initial data to the object.
    let initial_data = random_buffer();
    object.write_all(initial_data.as_slice())?;
    object.commit()?;
    object.seek(SeekFrom::Start(new_start_position as u64))?;

    // Partially overwrite that initial data with new data which extends past the initial data.
    let new_data = random_buffer();
    object.write_all(new_data.as_slice())?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Read all the data.
    let mut expected_data = initial_data;
    expected_data.splice(new_start_position.., new_data);
    let mut actual_data = vec![0u8; expected_data.len()];
    object.read_exact(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn truncate_object(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    // Write data to the object.
    let initial_data = random_buffer();
    object.write_all(initial_data.as_slice())?;
    object.commit()?;

    // Truncate the object.
    let new_size = MIN_BUFFER_SIZE as u64 / 2;
    object.set_len(new_size)?;

    assert_eq!(object.size().unwrap(), new_size);
    assert_eq!(object.seek(SeekFrom::Current(0))?, new_size);

    // Read data from the object.
    let expected_data = &initial_data[..new_size as usize];
    let mut actual_data = Vec::new();
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn extend_object(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    // Write data to the object.
    let initial_data = random_buffer();
    object.write_all(initial_data.as_slice())?;
    object.commit()?;

    let original_bytes = initial_data.len() as u64;
    let added_bytes = original_bytes;
    let new_size = original_bytes + added_bytes;

    // Truncate the object.
    object.set_len(new_size)?;

    // assert_eq!(object.size().unwrap(), new_size);
    assert_eq!(object.seek(SeekFrom::Current(0))?, original_bytes);

    // Read data from the object.
    let mut expected_data = initial_data;
    expected_data.resize(new_size as usize, 0);
    let mut actual_data = Vec::new();
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn compare_content_ids(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let initial_data = random_buffer();

    // Write data to the first object.
    let mut object = repo.insert(String::from("test1"));
    object.write_all(initial_data.as_slice())?;
    object.commit()?;
    let content_id1 = object.content_id().unwrap();
    drop(object);

    // Write the same data to the second object.
    let mut object = repo.insert(String::from("test2"));
    object.write_all(initial_data.as_slice())?;
    object.commit()?;
    let content_id2 = object.content_id().unwrap();
    drop(object);

    assert_eq!(content_id1, content_id2);

    // Write new data to the second object.
    let mut object = repo.object("test2").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.commit()?;
    let content_id2 = object.content_id().unwrap();

    assert_ne!(content_id1, content_id2);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn compare_contents_with_are_equal(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    let initial_data = random_buffer();

    // Write data to the object.
    object.write_all(initial_data.as_slice())?;
    object.commit()?;

    assert!(object
        .content_id()?
        .compare_contents(initial_data.as_slice())?);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn compare_unequal_contents_with_same_size(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    let initial_data = random_buffer();
    let modified_data = random_bytes(initial_data.len());

    // Write data to the object.
    object.write_all(initial_data.as_slice())?;
    object.commit()?;

    assert!(!object
        .content_id()?
        .compare_contents(modified_data.as_slice())?);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn compare_contents_which_are_smaller(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    let initial_data = random_buffer();
    let modified_data = &initial_data[..initial_data.len() / 2];

    // Write data to the object.
    object.write_all(initial_data.as_slice())?;
    object.commit()?;

    assert!(!object.content_id()?.compare_contents(modified_data)?);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn compare_contents_which_are_larger(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    let initial_data = random_buffer();
    let modified_data = [initial_data.clone(), random_buffer()].concat();

    // Write data to the object.
    object.write_all(initial_data.as_slice())?;
    object.commit()?;

    assert!(!object
        .content_id()?
        .compare_contents(modified_data.as_slice())?);

    Ok(())
}

#[test_case(common::FIXED_CONFIG.to_owned(); "with fixed-size chunking")]
#[test_case(common::ENCODING_CONFIG.to_owned(); "with encryption and compression")]
#[test_case(common::ZPAQ_CONFIG.to_owned(); "with ZPAQ chunking")]
#[test_case(common::FIXED_PACKING_SMALL_CONFIG.to_owned(); "with a pack size smaller than the chunk size")]
#[test_case(common::FIXED_PACKING_LARGE_CONFIG.to_owned(); "with a pack size larger than the chunk size")]
#[test_case(common::ZPAQ_PACKING_CONFIG.to_owned(); "with packing and ZPAQ chunking")]
fn verify_valid_object_is_valid(config: RepoConfig) -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_buffer().as_slice())?;
    object.commit()?;

    assert!(object.verify()?);
    Ok(())
}

#[test]
fn write_buffer_with_same_size_as_fixed_chunk_size() -> anyhow::Result<()> {
    let chunk_size = 1024 * 1024;

    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .chunking(Chunking::Fixed { size: chunk_size })
        .encryption(Encryption::None)
        .compression(Compression::None)
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;
    let mut object = repo.insert(String::from("test"));

    object.write_all(random_bytes(chunk_size as usize).as_slice())?;
    object.commit()?;

    assert_eq!(object.size().unwrap(), chunk_size as u64);
    Ok(())
}

#[test]
fn reading_seeking_with_uncommitted_changes_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(b"test data")?;
    let mut content = Vec::new();

    assert!(matches!(
        acid_store::Error::from(object.read(&mut content).unwrap_err()),
        acid_store::Error::TransactionInProgress
    ));

    assert!(matches!(
        acid_store::Error::from(object.seek(SeekFrom::Start(0)).unwrap_err()),
        acid_store::Error::TransactionInProgress
    ));

    Ok(())
}

#[test]
fn accessing_with_uncommitted_changes_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(b"test data")?;

    assert!(matches!(
        object.size(),
        Err(acid_store::Error::TransactionInProgress)
    ));

    assert!(matches!(
        object.content_id(),
        Err(acid_store::Error::TransactionInProgress)
    ));

    assert!(matches!(
        object.verify(),
        Err(acid_store::Error::TransactionInProgress)
    ));

    Ok(())
}

#[test]
fn truncating_with_uncommitted_changes_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(b"test data")?;

    assert!(matches!(
        object.set_len(0),
        Err(acid_store::Error::TransactionInProgress)
    ));

    Ok(())
}

#[test]
fn writing_from_another_instance_with_uncommitted_changes_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object1 = repo.insert(String::from("test"));
    object1.write_all(b"test data")?;

    let mut object2 = repo.object("test").unwrap();

    assert!(matches!(
        acid_store::Error::from(object2.write_all(b"test data").unwrap_err()),
        acid_store::Error::TransactionInProgress
    ));

    object1.commit()?;

    assert!(object2.write_all(b"test data").is_ok());

    Ok(())
}

#[test]
fn truncating_from_another_instance_with_uncommitted_changes_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object1 = repo.insert(String::from("test"));
    object1.write_all(b"test_data")?;
    object1.commit()?;

    let mut object2 = repo.object("test").unwrap();
    object2.write_all(b"test data")?;

    assert!(matches!(
        object1.set_len(0),
        Err(acid_store::Error::TransactionInProgress)
    ));

    object2.commit()?;

    assert!(object1.set_len(0).is_ok());

    Ok(())
}

#[test]
fn extending_from_another_instance_with_uncommitted_changes_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object1 = repo.insert(String::from("test"));

    let mut object2 = repo.object("test").unwrap();
    object2.write_all(b"test data")?;

    assert!(matches!(
        object1.set_len(10),
        Err(acid_store::Error::TransactionInProgress)
    ));

    object2.commit()?;

    assert!(object1.set_len(10).is_ok());

    Ok(())
}

#[test]
fn reading_seeking_from_another_instance_with_uncommitted_changes_is_ok() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object1 = repo.insert(String::from("test"));

    object1.write_all(b"test data")?;

    let mut object2 = repo.object("test").unwrap();
    let mut content = Vec::new();

    assert!(object2.seek(SeekFrom::Start(0)).is_ok());
    assert!(object2.read_to_end(&mut content).is_ok());

    Ok(())
}

#[test]
fn accessing_from_another_instance_with_uncommitted_changes_is_ok() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object1 = repo.insert(String::from("test"));

    object1.write_all(b"test data")?;

    let mut object2 = repo.object("test").unwrap();

    assert!(object2.size().is_ok());
    assert!(object2.content_id().is_ok());
    assert!(object2.verify().is_ok());

    Ok(())
}

#[test]
fn uncommitted_changes_are_not_visible_from_other_instances() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object1 = repo.insert(String::from("test"));
    let expected_content = random_buffer();

    object1.write_all(&expected_content)?;
    object1.flush()?;

    let mut object2 = repo.object("test").unwrap();
    let mut actual_content = Vec::new();
    object2.read_to_end(&mut actual_content)?;

    assert!(actual_content.is_empty());

    object1.commit()?;
    object2.read_to_end(&mut actual_content)?;

    assert_eq!(&actual_content, &expected_content);

    Ok(())
}

#[test]
fn accessing_once_repo_is_dropped_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object = repo.insert(String::from("test"));
    drop(repo);
    let mut content = Vec::new();

    assert!(matches!(
        object.size(),
        Err(acid_store::Error::InvalidObject)
    ));
    assert!(matches!(
        object.content_id(),
        Err(acid_store::Error::InvalidObject)
    ));
    assert!(matches!(
        object.verify(),
        Err(acid_store::Error::InvalidObject)
    ));
    assert!(matches!(
        object.set_len(0),
        Err(acid_store::Error::InvalidObject)
    ));
    assert!(matches!(
        acid_store::Error::from(object.seek(SeekFrom::Start(0)).unwrap_err()),
        acid_store::Error::InvalidObject,
    ));
    assert!(matches!(
        acid_store::Error::from(object.read(&mut content).unwrap_err()),
        acid_store::Error::InvalidObject,
    ));
    assert!(matches!(
        acid_store::Error::from(object.write(b"test data").unwrap_err()),
        acid_store::Error::InvalidObject,
    ));

    Ok(())
}

#[test]
fn accessing_once_object_is_removed_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object = repo.insert(String::from("test"));
    repo.remove("test");
    let mut content = Vec::new();

    assert!(matches!(
        object.size(),
        Err(acid_store::Error::InvalidObject)
    ));
    assert!(matches!(
        object.content_id(),
        Err(acid_store::Error::InvalidObject)
    ));
    assert!(matches!(
        object.verify(),
        Err(acid_store::Error::InvalidObject)
    ));
    assert!(matches!(
        object.set_len(0),
        Err(acid_store::Error::InvalidObject)
    ));
    assert!(matches!(
        acid_store::Error::from(object.seek(SeekFrom::Start(0)).unwrap_err()),
        acid_store::Error::InvalidObject,
    ));
    assert!(matches!(
        acid_store::Error::from(object.read(&mut content).unwrap_err()),
        acid_store::Error::InvalidObject,
    ));
    assert!(matches!(
        acid_store::Error::from(object.write(b"test data").unwrap_err()),
        acid_store::Error::InvalidObject,
    ));

    Ok(())
}

#[test]
fn converting_to_read_only_with_uncommitted_changes_errs() -> anyhow::Result<()> {
    let store_config = MemoryConfig::new();
    let mut repo: KeyRepo<String> = OpenOptions::new()
        .mode(OpenMode::CreateNew)
        .open(&store_config)?;

    let mut object = repo.insert(String::from("test"));
    object.write_all(b"test data")?;

    assert!(matches!(
        ReadOnlyObject::try_from(object),
        Err(acid_store::Error::TransactionInProgress)
    ));

    Ok(())
}
