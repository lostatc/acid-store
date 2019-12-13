/*
 * Copyright 2019 Garrett Powell
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

use std::io;

use tempfile::tempdir;

use disk_archive::ObjectArchive;

#[test]
fn data_is_persisted() -> io::Result<()> {
    let temp_dir = tempdir()?;
    let archive_path = temp_dir.path().join("archive");
    let mut archive = ObjectArchive::create(archive_path.as_path(), Default::default(), None)?;

    let expected_data = b"This is data.";
    let object = archive.write(&mut expected_data.as_ref())?;

    archive.insert("Test".to_string(), object);
    archive.commit()?;
    drop(archive);

    let archive = ObjectArchive::open(archive_path.as_path(), None)?;
    let object = archive.get(&"Test".to_string()).unwrap();
    let actual_data = archive.read_all(&object)?;

    assert_eq!(expected_data, &actual_data.as_ref());

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
