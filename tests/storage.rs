use std::io::Read;

use tempfile::tempdir;

use disk_archive::{Object, ObjectArchive, Result};

#[test]
fn metadata_is_persisted() -> Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path())?;

    let metadata = b"This is metadata.";
    let mut object = Object::new();
    object.metadata = metadata.to_vec();

    archive.insert("Test", object);
    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path())?;
    let object = archive.get("Test").unwrap();

    assert_eq!(object.metadata, metadata);

    Ok(())
}

#[test]
fn data_is_persisted() -> Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path())?;

    let expected_data = b"This is data.";
    let mut object = Object::new();
    object.data = archive.write(&mut expected_data.as_ref())?;

    archive.insert("Test", object);
    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path())?;
    let object = archive.get("Test").unwrap();
    let mut actual_data = Vec::new();
    archive.read(&object.data)?.read_to_end(&mut actual_data)?;

    assert_eq!(expected_data, &actual_data.as_ref());

    Ok(())
}

#[test]
fn uncommitted_changes_are_not_saved() -> Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path())?;

    let expected_data = b"This is data.";
    let mut object = Object::new();
    object.data = archive.write(&mut expected_data.as_ref())?;

    archive.insert("Test", object);
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path())?;

    assert_eq!(archive.get("Test"), None);

    Ok(())
}
