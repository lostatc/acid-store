#![cfg(all(feature = "encryption", feature = "compression"))]

use std::convert::TryFrom;
use std::io::{Read, Seek, SeekFrom, Write};

use acid_store::repo::key::KeyRepo;
use acid_store::repo::{Chunking, Commit, ReadOnlyObject, RepoConfig, RestoreSavepoint};
use common::*;
use rstest_reuse::{self, *};

mod common;

#[apply(object_config)]
fn read_written_data(#[case] repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let mut object = repo_object.object;
    let mut actual_data = Vec::new();

    object.write_all(&buffer)?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    assert_that!(&actual_data).is_equal_to(&buffer);
    assert_that!(&object.size()).is_ok_containing(buffer.len() as u64);

    Ok(())
}

#[apply(object_config)]
fn append_to_object(
    #[case] repo_object: RepoObject,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    object.write_all(&first_buffer)?;
    object.commit()?;

    object.seek(SeekFrom::End(0))?;

    object.write_all(&second_buffer)?;
    object.commit()?;

    let mut expected_data = first_buffer;
    expected_data.extend(&second_buffer);

    let mut actual_data = Vec::new();
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    assert_that!(&object.size()).is_ok_containing(expected_data.len() as u64);
    assert_that!(&actual_data).is_equal_to(&expected_data);

    Ok(())
}

#[apply(object_config)]
fn seek_and_read_data(#[case] repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    let mut actual_data = Vec::new();

    object.write_all(&buffer)?;
    object.commit()?;

    // Seek from start.
    object.seek(SeekFrom::Start(10))?;
    object.read_to_end(&mut actual_data)?;

    assert_that!(&actual_data.as_slice()).is_equal_to(&buffer[10..]);

    actual_data.clear();

    // Seek from end.
    object.seek(SeekFrom::End(10))?;
    object.read_to_end(&mut actual_data)?;
    let start_position = buffer.len() - 10;

    assert_that!(&actual_data.as_slice()).is_equal_to(&buffer[start_position..]);

    actual_data.clear();

    // Seek from current position.
    object.seek(SeekFrom::Start(10))?;
    object.seek(SeekFrom::Current(10))?;
    object.read_to_end(&mut actual_data)?;

    assert_that!(&actual_data.as_slice()).is_equal_to(&buffer[20..]);

    Ok(())
}

#[apply(object_config)]
fn seek_to_negative_offset(#[case] repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    // Write initial data to the object.
    object.write_all(&buffer)?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    assert_that!(&object.seek(SeekFrom::Current(-1))).is_err();

    Ok(())
}

#[apply(object_config)]
fn overwrite_written_data(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
    larger_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    // Write initial data to the object.
    object.write_all(&buffer)?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Overwrite that initial data with new data.
    object.write_all(&larger_buffer)?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Read the new data.
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;

    assert_that!(&actual_data).is_equal_to(&larger_buffer);

    Ok(())
}

#[apply(object_config)]
fn partially_overwrite_written_data(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
    smaller_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    // Write initial data to the object.
    object.write_all(&buffer)?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Partially overwrite that initial data with new data.
    object.write_all(&smaller_buffer)?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Read all the data.
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;

    let mut expected_data = buffer.clone();
    expected_data[..smaller_buffer.len()].copy_from_slice(&smaller_buffer);

    assert_that!(&actual_data).is_equal_to(expected_data);

    Ok(())
}

#[apply(object_config)]
fn partially_overwrite_and_grow_data(
    #[case] repo_object: RepoObject,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;
    let new_start_position = first_buffer.len() / 2;

    // Write initial data to the object.
    object.write_all(&first_buffer)?;
    object.commit()?;
    object.seek(SeekFrom::Start(new_start_position as u64))?;

    // Partially overwrite that initial data with new data which extends past the initial data.
    object.write_all(&second_buffer)?;
    object.commit()?;
    object.seek(SeekFrom::Start(0))?;

    // Read all the data.
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;

    let mut expected_data = first_buffer.clone();
    expected_data.splice(new_start_position.., second_buffer);

    assert_that!(&actual_data).is_equal_to(expected_data);

    Ok(())
}

#[apply(object_config)]
fn truncate_object(#[case] repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    // Write data to the object.
    object.write_all(&buffer)?;
    object.commit()?;

    // Truncate the object.
    let new_size = buffer.len() as u64 / 2;
    object.set_len(new_size)?;

    assert_that!(&object.size()).is_ok_containing(new_size);
    assert_that!(&object.seek(SeekFrom::Current(0))).is_ok_containing(new_size);

    // Read data from the object.
    let mut actual_data = Vec::new();
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    let expected_data = &buffer[..new_size as usize];

    assert_that!(&actual_data.as_slice()).is_equal_to(expected_data);

    Ok(())
}

#[apply(object_config)]
fn extend_object(#[case] repo_object: RepoObject, buffer: Vec<u8>) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    // Write data to the object.
    object.write_all(&buffer)?;
    object.commit()?;

    let original_bytes = buffer.len() as u64;
    let added_bytes = original_bytes;
    let new_size = original_bytes + added_bytes;

    // Extend the object.
    object.set_len(new_size)?;

    assert_that!(&object.size()).is_ok_containing(new_size);
    assert_that!(&object.seek(SeekFrom::Current(0))).is_ok_containing(original_bytes);

    // Read data from the object.
    let mut actual_data = Vec::new();
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    let mut expected_data = buffer.clone();
    expected_data.resize(new_size as usize, 0);

    assert_that!(&actual_data).is_equal_to(&expected_data);

    Ok(())
}

#[rstest]
fn extend_to_absurd_size(repo_object: RepoObject) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    let new_size = u64::MAX;

    object.set_len(new_size)?;

    assert_that!(&object.size()).is_ok_containing(new_size);

    Ok(())
}

#[apply(object_config)]
fn extend_then_append(
    #[case] repo_object: RepoObject,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    let mut expected_data = Vec::new();

    // Write data to the object.
    expected_data.extend_from_slice(&first_buffer);
    object.write_all(&first_buffer)?;
    object.commit()?;

    // Extend the object.
    object.set_len(first_buffer.len() as u64 * 2)?;
    expected_data.resize(first_buffer.len() * 2, 0);

    // Append more data to the object after the hole.
    expected_data.extend_from_slice(&second_buffer);
    object.seek(SeekFrom::End(0))?;
    object.write_all(&second_buffer)?;
    object.commit()?;

    assert_that!(&object.size()).is_ok_containing(expected_data.len() as u64);

    // Read data from the object.
    let mut actual_data = Vec::new();
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    assert_that!(&actual_data).is_equal_to(&expected_data);

    Ok(())
}

#[apply(object_config)]
fn extend_then_write_in_hole(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    // Extend the object before writing data.
    object.set_len(buffer.len() as u64 * 2)?;

    // Seek partway into the hole.
    object.seek(SeekFrom::Start(buffer.len() as u64 / 2))?;

    // Write in the middle of the hole.
    object.write_all(&buffer)?;
    object.commit()?;

    // Calculate the expected buffer;
    let mut expected_data = vec![0u8; buffer.len() * 2];
    let buffer_start = buffer.len() / 2;
    let buffer_end = buffer_start + buffer.len();
    expected_data[buffer_start..buffer_end].copy_from_slice(&buffer);

    // Read the actual data from the object.
    let mut actual_data = Vec::new();
    object.seek(SeekFrom::Start(0))?;
    object.read_to_end(&mut actual_data)?;

    assert_that!(&actual_data).is_equal_to(&expected_data);

    Ok(())
}

#[rstest]
fn check_file_stats_with_holes(
    repo_object: RepoObject,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    let first_buffer_size = first_buffer.len() as u64;
    let second_buffer_size = second_buffer.len() as u64;
    let first_hole_size = 100u64;
    let second_hole_size = 2000u64;

    // Write some initial data.
    object.write_all(&first_buffer)?;
    object.commit()?;

    // Write the first hole.
    object.set_len(object.size()? + first_hole_size)?;

    // Write some more data.
    object.seek(SeekFrom::End(0))?;
    object.write_all(&second_buffer)?;
    object.commit()?;

    // Write a second hole.
    object.set_len(object.size()? + second_hole_size)?;

    let stats = object.stats()?;
    let expected_apparent_size =
        first_buffer_size + first_hole_size + second_buffer_size + second_hole_size;
    let expected_actual_size = first_buffer_size + second_buffer_size;
    let expected_holes = &[
        first_buffer_size..(first_buffer_size + first_hole_size),
        (first_buffer_size + first_hole_size + second_buffer_size)
            ..(first_buffer_size + first_hole_size + second_buffer_size + second_hole_size),
    ];

    assert_that!(&stats.apparent_size()).is_equal_to(object.size()?);
    assert_that!(&stats.apparent_size()).is_equal_to(expected_apparent_size);
    assert_that!(&stats.actual_size()).is_equal_to(expected_actual_size);
    assert_that!(&stats.holes()).is_equal_to(&expected_holes[..]);

    Ok(())
}

#[apply(repo_config)]
fn compare_content_ids(
    #[case] mut repo: KeyRepo<String>,
    #[from(buffer)] first_buffer: Vec<u8>,
    #[from(buffer)] second_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    // Write data to the first object.
    let mut object = repo.insert(String::from("test1"));
    object.write_all(&first_buffer)?;
    object.commit()?;
    let content_id1 = object.content_id().unwrap();
    drop(object);

    // Write the same data to the second object.
    let mut object = repo.insert(String::from("test2"));
    object.write_all(&first_buffer)?;
    object.commit()?;
    let content_id2 = object.content_id().unwrap();
    drop(object);

    assert_that!(&content_id1).is_equal_to(&content_id2);

    // Write new data to the second object.
    let mut object = repo.object("test2").unwrap();
    object.write_all(&second_buffer)?;
    object.commit()?;
    let content_id2 = object.content_id().unwrap();

    assert_that!(&content_id1).is_not_equal_to(&content_id2);

    Ok(())
}

#[rstest]
fn content_ids_treat_holes_specially(
    repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut repo = repo_object.repo;
    let mut hole_object = repo_object.object;

    let hole_size = 200usize;

    hole_object.write_all(&buffer)?;
    hole_object.commit()?;
    hole_object.set_len((buffer.len() + hole_size) as u64)?;
    let hole_content_id = hole_object.content_id()?;
    hole_object.seek(SeekFrom::Start(0))?;

    let mut null_bytes_object = repo.insert(String::from("test"));
    null_bytes_object.write_all(&buffer)?;
    null_bytes_object.write_all(&vec![0u8; hole_size])?;
    null_bytes_object.commit()?;
    let null_bytes_content_id = null_bytes_object.content_id()?;
    null_bytes_object.seek(SeekFrom::Start(0))?;

    assert_that!(hole_content_id).is_not_equal_to(&null_bytes_content_id);
    assert_that!(null_bytes_content_id.compare_contents(hole_object))
        .is_ok()
        .is_true();
    assert_that!(hole_content_id.compare_contents(null_bytes_object))
        .is_ok()
        .is_true();

    Ok(())
}

#[apply(object_config)]
fn compare_contents_with_are_equal(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    // Write data to the object.
    object.write_all(&buffer)?;
    object.commit()?;

    assert_that!(&object.content_id()?.compare_contents(buffer.as_slice())).is_ok_containing(true);

    Ok(())
}

#[apply(object_config)]
fn compare_unequal_contents_with_same_size(
    #[case] repo_object: RepoObject,
    #[from(fixed_buffer)] first_buffer: Vec<u8>,
    #[from(fixed_buffer)] second_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    // Write data to the object.
    object.write_all(&first_buffer)?;
    object.commit()?;

    assert_that!(&object
        .content_id()?
        .compare_contents(second_buffer.as_slice()))
    .is_ok_containing(false);

    Ok(())
}

#[apply(object_config)]
fn compare_contents_which_are_smaller(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    let smaller_buffer = &buffer[..buffer.len() / 2];

    // Write data to the object.
    object.write_all(&buffer)?;
    object.commit()?;

    assert_that!(&object.content_id()?.compare_contents(smaller_buffer)).is_ok_containing(false);

    Ok(())
}

#[apply(object_config)]
fn compare_contents_which_are_larger(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    let mut larger_buffer = buffer.clone();
    larger_buffer.resize(buffer.len() * 2, 0);

    // Write data to the object.
    object.write_all(&buffer)?;
    object.commit()?;

    assert_that!(&object
        .content_id()?
        .compare_contents(larger_buffer.as_slice()))
    .is_ok_containing(false);

    Ok(())
}

#[apply(object_config)]
fn verify_valid_object_is_valid(
    #[case] repo_object: RepoObject,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    object.write_all(&buffer)?;
    object.commit()?;

    assert_that!(&object.verify()).is_ok_containing(true);

    Ok(())
}

#[rstest]
fn write_buffer_with_same_size_as_fixed_chunk_size(
    #[with(1024 * 1024)] fixed_buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut config = RepoConfig::default();
    config.chunking = Chunking::Fixed { size: 1024 * 1024 };
    let repo_object = RepoObject::new(config)?;
    let mut object = repo_object.object;

    object.write_all(&fixed_buffer)?;
    object.commit()?;

    assert_that!(&object.size()).is_ok_containing(1024 * 1024);

    Ok(())
}

#[rstest]
fn reading_seeking_with_uncommitted_changes_errs(repo_object: RepoObject) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    object.write_all(b"test data")?;
    let mut content = Vec::new();

    assert_that!(&object.read(&mut content).map_err(acid_store::Error::from))
        .is_err_variant(acid_store::Error::TransactionInProgress);
    assert_that!(&object
        .seek(SeekFrom::Start(0))
        .map_err(acid_store::Error::from))
    .is_err_variant(acid_store::Error::TransactionInProgress);

    Ok(())
}

#[rstest]
fn accessing_with_uncommitted_changes_errs(repo_object: RepoObject) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    object.write_all(b"test data")?;

    assert_that!(object.size()).is_err_variant(acid_store::Error::TransactionInProgress);
    assert_that!(object.stats()).is_err_variant(acid_store::Error::TransactionInProgress);
    assert_that!(object.content_id()).is_err_variant(acid_store::Error::TransactionInProgress);
    assert_that!(object.verify()).is_err_variant(acid_store::Error::TransactionInProgress);

    Ok(())
}

#[rstest]
fn truncating_with_uncommitted_changes_errs(repo_object: RepoObject) -> anyhow::Result<()> {
    let mut object = repo_object.object;

    object.write_all(b"test data")?;

    assert_that!(object.set_len(0)).is_err_variant(acid_store::Error::TransactionInProgress);

    Ok(())
}

#[rstest]
fn writing_from_another_instance_with_uncommitted_changes_errs(
    mut repo: KeyRepo<String>,
) -> anyhow::Result<()> {
    let mut object1 = repo.insert(String::from("test"));
    object1.write_all(b"test data")?;

    let mut object2 = repo.object("test").unwrap();

    assert_that!(object2
        .write_all(b"test data")
        .map_err(acid_store::Error::from))
    .is_err_variant(acid_store::Error::TransactionInProgress);

    object1.commit()?;

    assert_that!(object2.write_all(b"test data")).is_ok();

    Ok(())
}

#[rstest]
fn truncating_from_another_instance_with_uncommitted_changes_errs(
    mut repo: KeyRepo<String>,
) -> anyhow::Result<()> {
    let mut object1 = repo.insert(String::from("test"));
    object1.write_all(b"test_data")?;
    object1.commit()?;

    let mut object2 = repo.object("test").unwrap();
    object2.write_all(b"test data")?;

    assert_that!(object1.set_len(0)).is_err_variant(acid_store::Error::TransactionInProgress);

    object2.commit()?;

    assert_that!(object1.set_len(0)).is_ok();

    Ok(())
}

#[rstest]
fn extending_from_another_instance_with_uncommitted_changes_errs(
    mut repo: KeyRepo<String>,
) -> anyhow::Result<()> {
    let mut object1 = repo.insert(String::from("test"));

    let mut object2 = repo.object("test").unwrap();
    object2.write_all(b"test data")?;

    assert_that!(object1.set_len(10)).is_err_variant(acid_store::Error::TransactionInProgress);

    object2.commit()?;

    assert_that!(object1.set_len(10)).is_ok();

    Ok(())
}

#[rstest]
fn reading_seeking_from_another_instance_with_uncommitted_changes_is_ok(
    mut repo: KeyRepo<String>,
) -> anyhow::Result<()> {
    let mut object1 = repo.insert(String::from("test"));

    object1.write_all(b"test data")?;

    let mut object2 = repo.object("test").unwrap();
    let mut content = Vec::new();

    assert_that!(object2.seek(SeekFrom::Start(0))).is_ok();
    assert_that!(object2.read_to_end(&mut content)).is_ok();

    Ok(())
}

#[rstest]
fn accessing_from_another_instance_with_uncommitted_changes_is_ok(
    mut repo: KeyRepo<String>,
) -> anyhow::Result<()> {
    let mut object1 = repo.insert(String::from("test"));

    object1.write_all(b"test data")?;

    let mut object2 = repo.object("test").unwrap();

    assert_that!(object2.size()).is_ok();
    assert_that!(object2.stats()).is_ok();
    assert_that!(object2.content_id()).is_ok();
    assert_that!(object2.verify()).is_ok();

    Ok(())
}

#[rstest]
fn uncommitted_changes_are_not_visible_from_other_instances(
    mut repo: KeyRepo<String>,
    buffer: Vec<u8>,
) -> anyhow::Result<()> {
    let mut object1 = repo.insert(String::from("test"));

    object1.write_all(&buffer)?;
    object1.flush()?;

    let mut object2 = repo.object("test").unwrap();
    let mut actual_content = Vec::new();
    object2.read_to_end(&mut actual_content)?;

    assert_that!(actual_content).is_empty();

    object1.commit()?;
    object2.read_to_end(&mut actual_content)?;

    assert_that!(&actual_content).is_equal_to(&buffer);

    Ok(())
}

#[rstest]
fn accessing_once_repo_is_dropped_errs(repo_object: RepoObject) -> anyhow::Result<()> {
    let mut object = repo_object.object;
    drop(repo_object.repo);

    let mut content = Vec::new();

    assert_that!(object.size()).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.content_id()).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.stats()).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.verify()).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.set_len(0)).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object
        .seek(SeekFrom::Start(0))
        .map_err(acid_store::Error::from))
    .is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.read(&mut content).map_err(acid_store::Error::from))
        .is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.write(b"test data").map_err(acid_store::Error::from))
        .is_err_variant(acid_store::Error::InvalidObject);

    Ok(())
}

#[rstest]
fn accessing_once_object_is_removed_errs(repo_object: RepoObject) -> anyhow::Result<()> {
    let RepoObject {
        mut object,
        mut repo,
        key,
    } = repo_object;
    repo.remove(&key);

    let mut content = Vec::new();

    assert_that!(object.size()).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.content_id()).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.stats()).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.verify()).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.set_len(0)).is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object
        .seek(SeekFrom::Start(0))
        .map_err(acid_store::Error::from))
    .is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.read(&mut content).map_err(acid_store::Error::from))
        .is_err_variant(acid_store::Error::InvalidObject);
    assert_that!(object.write(b"test data").map_err(acid_store::Error::from))
        .is_err_variant(acid_store::Error::InvalidObject);

    Ok(())
}

#[rstest]
fn converting_to_read_only_with_uncommitted_changes_errs(
    repo_object: RepoObject,
) -> anyhow::Result<()> {
    let mut object = repo_object.object;
    object.write_all(b"test data")?;

    assert_that!(&ReadOnlyObject::try_from(object))
        .is_err_variant(acid_store::Error::TransactionInProgress);

    Ok(())
}

#[rstest]
fn rolling_back_repo_invalidates_objects(repo_object: RepoObject) -> anyhow::Result<()> {
    let RepoObject {
        mut repo, object, ..
    } = repo_object;

    assert_that!(object.is_valid()).is_true();

    repo.rollback()?;

    assert_that!(object.is_valid()).is_false();
    assert_that!(object.size()).is_err_variant(acid_store::Error::InvalidObject);

    Ok(())
}

#[rstest]
fn restoring_to_savepoint_invalidates_objects(repo_object: RepoObject) -> anyhow::Result<()> {
    let RepoObject {
        mut repo, object, ..
    } = repo_object;

    assert_that!(object.is_valid()).is_true();

    let savepoint = repo.savepoint()?;
    repo.restore(&savepoint)?;

    assert_that!(object.is_valid()).is_false();
    assert_that!(object.size()).is_err_variant(acid_store::Error::InvalidObject);

    Ok(())
}
