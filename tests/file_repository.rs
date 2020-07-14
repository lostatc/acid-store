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

use std::collections::HashMap;
use std::fs::{create_dir, File};
use std::io::{Read, Write};

use maplit::hashmap;
#[cfg(all(linux, feature = "file-metadata"))]
use posix_acl::{PosixACL, Qualifier as PosixQualifier};
use relative_path::RelativePathBuf;
use tempfile::tempdir;

use acid_store::repo::file::{Entry, FileRepo, NoMetadata, NoSpecialType};
use acid_store::repo::{ConvertRepo, OpenOptions};
use acid_store::store::MemoryStore;
use common::assert_contains_all;
#[cfg(all(unix, feature = "file-metadata"))]
use {
    acid_store::repo::file::{
        AccessQualifier, CommonMetadata, FileType, UnixMetadata, UnixSpecialType,
    },
    nix::sys::stat::{Mode, SFlag},
    nix::unistd::mkfifo,
    std::fs::read_link,
    std::os::unix::fs::{symlink, MetadataExt},
    std::path::Path,
    std::time::SystemTime,
};

mod common;

fn create_repo() -> acid_store::Result<FileRepo<MemoryStore>> {
    OpenOptions::new(MemoryStore::new()).create_new()
}

#[test]
fn open_repository() -> anyhow::Result<()> {
    let mut repo = create_repo()?;
    repo.commit()?;
    let store = repo.into_repo()?.into_store();
    OpenOptions::new(store).open::<FileRepo<_>>()?;
    Ok(())
}

#[test]
fn creating_existing_file_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.create("home", &Entry::directory())?;
    let result = repository.create("home", &Entry::directory());

    assert!(matches!(
        result.unwrap_err(),
        acid_store::Error::AlreadyExists
    ));
    Ok(())
}

#[test]
fn creating_file_without_parent_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    // Creating a directory without a parent fails.
    let result = repository.create("home/lostatc", &Entry::directory());
    assert!(matches!(
        result.unwrap_err(),
        acid_store::Error::InvalidPath
    ));

    // Creating a directory as a child of a file fails.
    repository.create("home", &Entry::file())?;
    let result = repository.create("home/lostatc", &Entry::directory());
    assert!(matches!(
        result.unwrap_err(),
        acid_store::Error::InvalidPath
    ));

    Ok(())
}

#[test]
fn create_parents() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.create_parents("home/lostatc/test", &Entry::file())?;

    assert!(repository.entry("home/lostatc/test")?.is_file());
    assert!(repository.entry("home/lostatc")?.is_directory());
    assert!(repository.entry("home")?.is_directory());

    Ok(())
}

#[test]
fn create_parent_of_top_level_file() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    repository.create_parents("home", &Entry::directory())?;

    assert!(repository.entry("home")?.is_directory());
    Ok(())
}

#[test]
fn removing_nonexistent_path_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let result = repository.remove("home");

    assert!(matches!(result, Err(acid_store::Error::NotFound)));
    Ok(())
}

#[test]
fn removing_non_empty_directory_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create_parents("home/lostatc", &Entry::directory())?;
    let result = repository.remove("home");

    assert!(matches!(result, Err(acid_store::Error::NotEmpty)));
    Ok(())
}

#[test]
fn remove_file() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create("home", &Entry::directory())?;
    repository.remove("home")?;

    assert!(!repository.exists("home"));
    Ok(())
}

#[test]
fn remove_tree() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create_parents("home/lostatc/test", &Entry::file())?;
    repository.remove_tree("home")?;

    assert!(!repository.exists("home"));
    assert!(!repository.exists("home/lostatc"));
    assert!(!repository.exists("home/lostatc/test"));
    Ok(())
}

#[test]
fn remove_tree_without_descendants() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create("home", &Entry::directory())?;
    repository.remove_tree("home")?;

    assert!(!repository.exists("home"));
    Ok(())
}

#[test]
fn setting_metadata_on_nonexistent_file_errs() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    let result = repository.set_metadata("file", None);

    assert!(matches!(result, Err(acid_store::Error::NotFound)));
    Ok(())
}

#[test]
#[cfg(feature = "file-metadata")]
fn set_metadata() -> anyhow::Result<()> {
    let mut repository: FileRepo<_, NoSpecialType, CommonMetadata> =
        OpenOptions::new(MemoryStore::new()).create_new()?;

    let expected_metadata = CommonMetadata {
        modified: SystemTime::UNIX_EPOCH,
        accessed: SystemTime::UNIX_EPOCH,
    };
    repository.create("file", &Entry::file())?;
    repository.set_metadata("file", Some(expected_metadata.clone()))?;
    let actual_metadata = repository.entry("file")?.metadata;

    assert_eq!(actual_metadata, Some(expected_metadata));
    Ok(())
}

#[test]
fn open_file() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create("file", &Entry::file())?;
    let mut object = repository.open_mut("file")?;

    object.write_all(b"expected data")?;
    object.flush()?;
    drop(object);

    let mut object = repository.open("file")?;
    let mut actual_data = Vec::new();
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data, b"expected data");
    Ok(())
}

#[test]
fn copied_file_has_same_contents() -> anyhow::Result<()> {
    let mut repository = create_repo()?;

    let expected_data = b"expected";
    let mut actual_data = Vec::new();

    // Add a file entry and write data to it.
    repository.create("source", &Entry::file())?;
    let mut object = repository.open_mut("source")?;
    object.write_all(expected_data)?;
    object.flush()?;
    drop(object);

    // Copy the file entry.
    repository.copy("source", "dest")?;

    let mut object = repository.open("dest")?;
    object.read_to_end(&mut actual_data)?;

    assert_eq!(actual_data, expected_data);
    Ok(())
}

#[test]
fn copy_file_with_invalid_destination() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create("source", &Entry::file())?;

    assert!(matches!(
        repository.copy("source", "nonexistent/dest"),
        Err(acid_store::Error::InvalidPath)
    ));

    repository.create("dest", &Entry::file())?;

    assert!(matches!(
        repository.copy("source", "dest"),
        Err(acid_store::Error::AlreadyExists)
    ));

    Ok(())
}

#[test]
fn copy_tree() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create_parents("source/file1", &Entry::file())?;
    repository.create_parents("source/directory/file2", &Entry::file())?;

    repository.copy_tree("source", "dest")?;

    assert!(repository.entry("dest/file1")?.is_file());
    assert!(repository.entry("dest/directory/file2")?.is_file());

    Ok(())
}

#[test]
fn copy_tree_which_is_a_file() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create("source", &Entry::file())?;

    repository.copy_tree("source", "dest")?;

    assert!(repository.entry("dest")?.is_file());

    Ok(())
}

#[test]
fn list_children() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create_parents("root/child1", &Entry::file())?;
    repository.create_parents("root/child2/descendant", &Entry::file())?;

    let actual = repository.list("root").unwrap();
    let expected = vec![
        RelativePathBuf::from("root/child1"),
        RelativePathBuf::from("root/child2"),
    ];

    assert_contains_all(actual, expected);
    Ok(())
}

#[test]
fn list_children_of_a_file() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create("file", &Entry::file())?;

    let result = repository.list("file").unwrap();

    assert_contains_all(result, Vec::new());

    Ok(())
}

#[test]
fn walk_descendants() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create_parents("root/child1", &Entry::file())?;
    repository.create_parents("root/child2/descendant", &Entry::file())?;

    let actual = repository.walk("root").unwrap();
    let expected = vec![
        RelativePathBuf::from("root/child1"),
        RelativePathBuf::from("root/child2"),
        RelativePathBuf::from("root/child2/descendant"),
    ];

    assert_contains_all(actual, expected);
    Ok(())
}

#[test]
fn walk_descendants_of_a_file() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create("file", &Entry::file())?;

    let result = repository.walk("file").unwrap();

    assert_contains_all(result, Vec::new());

    Ok(())
}

#[test]
fn archive_file() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let source_path = temp_dir.as_ref().join("source");
    let mut source_file = File::create(&source_path)?;
    source_file.write_all(b"file contents")?;
    source_file.flush()?;

    let mut repository = create_repo()?;
    repository.archive(&source_path, "dest")?;

    let mut object = repository.open("dest")?;
    let mut actual_contents = Vec::new();
    object.read_to_end(&mut actual_contents)?;

    assert_eq!(actual_contents, b"file contents");
    Ok(())
}

#[test]
#[cfg(all(unix, feature = "file-metadata"))]
fn archive_unix_special_files() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let fifo_path = temp_dir.as_ref().join("fifo");
    let symlink_path = temp_dir.as_ref().join("symlink");
    let device_path = Path::new("/dev/null");

    mkfifo(&fifo_path, Mode::S_IRWXU)?;
    symlink("/dev/null", &symlink_path)?;

    let mut repository: FileRepo<_, _, NoMetadata> =
        OpenOptions::new(MemoryStore::new()).create_new()?;

    repository.create("dest", &Entry::directory())?;
    repository.archive(fifo_path, "dest/fifo")?;
    repository.archive(symlink_path, "dest/symlink")?;
    repository.archive(device_path, "dest/device")?;

    let fifo_entry = repository.entry("dest/fifo")?;
    let symlink_entry = repository.entry("dest/symlink")?;
    let device_entry = repository.entry("dest/device")?;

    assert_eq!(fifo_entry.file_type, UnixSpecialType::NamedPipe.into());
    assert_eq!(
        symlink_entry.file_type,
        UnixSpecialType::SymbolicLink {
            target: "/dev/null".into()
        }
        .into()
    );
    assert_eq!(
        device_entry.file_type,
        UnixSpecialType::CharacterDevice { major: 1, minor: 3 }.into()
    );
    Ok(())
}

#[test]
fn archiving_file_with_existing_dest_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;

    let mut repository = create_repo()?;
    repository.create("dest", &Entry::file())?;
    let result = repository.archive(&source_path, "dest");

    assert!(matches!(result, Err(acid_store::Error::AlreadyExists)));
    Ok(())
}

#[test]
fn archive_tree() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let source_path = temp_dir.as_ref().join("source");

    create_dir(&source_path)?;
    File::create(&source_path.join("file1"))?;
    create_dir(&source_path.join("directory"))?;
    File::create(&source_path.join("directory/file2"))?;

    let mut repository = create_repo()?;
    repository.archive_tree(&source_path, "dest")?;

    assert!(repository.entry("dest")?.is_directory());
    assert!(repository.entry("dest/file1")?.is_file());
    assert!(repository.entry("dest/directory")?.is_directory());
    assert!(repository.entry("dest/directory/file2")?.is_file());
    Ok(())
}

#[test]
fn extract_file() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let dest_path = temp_dir.as_ref().join("dest");

    let mut repository = create_repo()?;
    repository.create("source", &Entry::file())?;
    let mut object = repository.open_mut("source")?;
    object.write_all(b"file contents")?;
    object.flush()?;
    drop(object);
    repository.extract("source", &dest_path)?;

    let mut actual_contents = Vec::new();
    let mut dest_file = File::open(&dest_path)?;
    dest_file.read_to_end(&mut actual_contents)?;

    assert_eq!(actual_contents, b"file contents");
    Ok(())
}

#[test]
#[cfg(all(unix, feature = "file-metadata"))]
fn extract_unix_special_files() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let fifo_path = temp_dir.as_ref().join("fifo");
    let symlink_path = temp_dir.as_ref().join("symlink");
    let device_path = temp_dir.as_ref().join("device");

    let mut repository: FileRepo<_, _, NoMetadata> =
        OpenOptions::new(MemoryStore::new()).create_new()?;

    repository.create("fifo", &Entry::special(UnixSpecialType::NamedPipe))?;
    repository.create(
        "symlink",
        &Entry::special(UnixSpecialType::SymbolicLink {
            target: "/dev/null".into(),
        }),
    )?;
    repository.create(
        "device",
        &Entry::special(UnixSpecialType::CharacterDevice { major: 1, minor: 3 }),
    )?;

    // The device won't be extracted unless the user has sufficient permissions. In this case, the
    // operation is supposed to silently fail. Assuming the tests are being run without root
    // permissions, we attempt to extract the device to ensure it doesn't return an error, but we
    // don't check to see if it was created.
    repository.extract("fifo", &fifo_path)?;
    repository.extract("symlink", &symlink_path)?;
    repository.extract("device", &device_path)?;

    assert!(
        SFlag::from_bits(fifo_path.metadata()?.mode() & SFlag::S_IFMT.bits())
            .unwrap()
            .contains(SFlag::S_IFIFO)
    );
    assert_eq!(
        read_link(&symlink_path)?,
        Path::new("/dev/null").to_path_buf()
    );

    Ok(())
}

#[test]
fn extracting_file_with_existing_dest_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let dest_path = temp_dir.as_ref().join("dest");
    File::create(&dest_path)?;

    let mut repository = create_repo()?;
    repository.create("source", &Entry::file())?;
    let result = repository.extract("source", &dest_path);

    assert!(matches!(result, Err(acid_store::Error::AlreadyExists)));
    Ok(())
}

#[test]
fn extract_tree() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let dest_path = temp_dir.as_ref().join("dest");

    let mut repository = create_repo()?;
    repository.create("source", &Entry::directory())?;
    repository.create("source/file1", &Entry::file())?;
    repository.create("source/directory", &Entry::directory())?;
    repository.create("source/directory/file2", &Entry::file())?;

    repository.extract_tree("source", &dest_path)?;

    assert!(dest_path.join("file1").is_file());
    assert!(dest_path.join("directory").is_dir());
    assert!(dest_path.join("directory/file2").is_file());
    Ok(())
}

#[test]
#[cfg(all(unix, feature = "file-metadata"))]
fn write_unix_metadata() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let dest_path = temp_dir.as_ref().join("dest");

    let mut repository: FileRepo<_, NoSpecialType, UnixMetadata> =
        OpenOptions::new(MemoryStore::new()).create_new()?;

    // This does not test extended attributes because user extended attributes are not supported
    // on tmpfs, which is most likely where the temporary directory will be created.

    let entry_metadata = UnixMetadata {
        mode: 0o666,
        modified: SystemTime::UNIX_EPOCH,
        accessed: SystemTime::UNIX_EPOCH,
        user: 1000,
        group: 1000,
        attributes: HashMap::new(),
        acl: hashmap! { AccessQualifier::User(1001) => 0o777 },
    };
    let entry = Entry {
        file_type: FileType::File,
        metadata: Some(entry_metadata.clone()),
    };

    repository.create("source", &entry)?;
    repository.extract("source", &dest_path)?;
    let dest_metadata = dest_path.metadata()?;

    assert_eq!(
        dest_metadata.mode() & entry_metadata.mode,
        entry_metadata.mode
    );
    assert_eq!(dest_metadata.modified()?, entry_metadata.modified);
    assert_eq!(dest_metadata.accessed()?, entry_metadata.accessed);

    #[cfg(linux)]
    {
        let dest_acl = PosixACL::new(dest_metadata.mode());
        assert_eq!(dest_acl.get(PosixQualifier::User(1001)), Some(0o777));
    }

    Ok(())
}

#[test]
#[cfg(all(unix, feature = "file-metadata"))]
fn read_unix_metadata() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;

    #[cfg(linux)]
    {
        let mut dest_acl = PosixACL::new(source_path.metadata()?.mode());
        dest_acl.set(PosixQualifier::User(1001), 0o777);
        dest_acl.write_acl(&source_path)?;
    }

    let mut repository: FileRepo<_, NoSpecialType, UnixMetadata> =
        OpenOptions::new(MemoryStore::new()).create_new()?;

    repository.archive(&source_path, "dest")?;
    let entry = repository.entry("dest")?;
    let entry_metadata = entry.metadata.unwrap();
    let source_metadata = source_path.metadata()?;

    // This does not test extended attributes because user extended attributes are not supported
    // on tmpfs, which is most likely where the temporary directory will be created.

    assert_eq!(entry_metadata.mode, source_metadata.mode());
    assert_eq!(entry_metadata.modified, source_metadata.modified()?);
    assert_eq!(entry_metadata.user, source_metadata.uid());
    assert_eq!(entry_metadata.group, source_metadata.gid());

    #[cfg(linux)]
    {
        assert_eq!(
            entry_metadata.acl,
            hashmap! { AccessQualifier::User(1001) => 0o777 }
        );
    }

    Ok(())
}

#[test]
#[cfg(all(unix, feature = "file-metadata"))]
fn write_common_metadata() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let dest_path = temp_dir.as_ref().join("dest");

    let mut repository: FileRepo<_, NoSpecialType, CommonMetadata> =
        OpenOptions::new(MemoryStore::new()).create_new()?;

    let entry_metadata = CommonMetadata {
        modified: SystemTime::UNIX_EPOCH,
        accessed: SystemTime::UNIX_EPOCH,
    };
    let entry = Entry {
        file_type: FileType::File,
        metadata: Some(entry_metadata.clone()),
    };

    repository.create("source", &entry)?;
    repository.extract("source", &dest_path)?;
    let dest_metadata = dest_path.metadata()?;

    assert_eq!(dest_metadata.modified()?, entry_metadata.modified);
    assert_eq!(dest_metadata.accessed()?, entry_metadata.accessed);

    Ok(())
}

#[test]
#[cfg(all(unix, feature = "file-metadata"))]
fn read_common_metadata() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let source_path = temp_dir.as_ref().join("source");
    File::create(&source_path)?;

    let mut repository: FileRepo<_, NoSpecialType, CommonMetadata> =
        OpenOptions::new(MemoryStore::new()).create_new()?;

    repository.archive(&source_path, "dest")?;
    let entry = repository.entry("dest")?;
    let entry_metadata = entry.metadata.unwrap();
    let source_metadata = source_path.metadata()?;

    assert_eq!(entry_metadata.modified, source_metadata.modified()?);

    Ok(())
}

#[test]
fn verify_valid_repository_is_valid() -> anyhow::Result<()> {
    let mut repository = create_repo()?;
    repository.create_parents("home/lostatc/file", &Entry::file())?;

    assert!(repository.verify()?.is_empty());
    Ok(())
}
