/*
 * Copyright 2019-2020 Garrett Powell
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

use common::{create_repo, random_buffer, random_bytes, MIN_BUFFER_SIZE};

mod common;

#[test]
fn read_written_data() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());

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

#[test]
fn seek_and_read_data() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".to_string());

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

#[test]
fn seek_to_negative_offset() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());

    // Write initial data to the object.
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    object.seek(SeekFrom::Start(0))?;

    assert!(object.seek(SeekFrom::Current(-1)).is_err());
    Ok(())
}

#[test]
fn overwrite_written_data() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
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
    let mut repository = create_repo()?;
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

#[test]
fn partially_overwrite_and_grow_data() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());
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

#[test]
fn truncate_object() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let mut object = repository.insert("Test".into());

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

#[test]
fn compare_content_ids() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let initial_data = random_buffer();

    // Write data to the first object.
    let mut object = repository.insert("Test1".into());
    object.write_all(initial_data.as_slice())?;
    object.flush()?;
    let content_id1 = object.content_id();

    // Write the same data to the second object.
    let mut object = repository.insert("Test2".into());
    object.write_all(initial_data.as_slice())?;
    object.flush()?;
    let content_id2 = object.content_id();

    assert_eq!(content_id1, content_id2);

    // Write new data to the second object.
    let mut object = repository.get("Test2").unwrap();
    object.write_all(random_buffer().as_slice())?;
    object.flush()?;
    let content_id2 = object.content_id();

    assert_ne!(content_id1, content_id2);

    Ok(())
}
