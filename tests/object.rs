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

use std::io::{Read, Seek, SeekFrom, Write};

use test_case::test_case;

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{Chunking, Compression, Encryption, OpenMode, OpenOptions, RepoConfig};
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
    object.flush()?;
    object.seek(SeekFrom::Start(0))?;
    object.read_exact(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);
    assert_eq!(object.size(), expected_data.len() as u64);

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
    object.flush()?;

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
    object.flush()?;
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
    object.flush()?;
    object.seek(SeekFrom::Start(new_start_position as u64))?;

    // Partially overwrite that initial data with new data which extends past the initial data.
    let new_data = random_buffer();
    object.write_all(new_data.as_slice())?;
    object.flush()?;
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
    object.flush()?;

    // Truncate the object.
    let new_size = MIN_BUFFER_SIZE as u64 / 2;
    object.truncate(new_size)?;

    assert_eq!(object.size(), new_size);
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
    object.flush()?;
    let content_id1 = object.content_id();
    drop(object);

    // Write the same data to the second object.
    let mut object = repo.insert(String::from("test2"));
    object.write_all(initial_data.as_slice())?;
    object.flush()?;
    let content_id2 = object.content_id();
    drop(object);

    assert_eq!(content_id1, content_id2);

    // Write new data to the second object.
    let mut object = repo.object_mut("test2").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    let content_id2 = object.content_id();

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
    object.flush()?;

    assert!(object.compare_contents(initial_data.as_slice())?);

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
    object.flush()?;

    assert!(!object.compare_contents(modified_data.as_slice())?);

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
    object.flush()?;

    assert!(!object.compare_contents(modified_data)?);

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
    object.flush()?;

    assert!(!object.compare_contents(modified_data.as_slice())?);

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
    object.flush()?;

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
    object.flush()?;

    assert_eq!(object.size(), chunk_size as u64);
    Ok(())
}
