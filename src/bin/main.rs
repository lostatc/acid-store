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

use std::io::Read;
use std::path::PathBuf;

use disk_archive::{Archive, ArchiveObject, Result};

fn main() {
    // create().unwrap();
    read().unwrap();
}

fn read() -> Result<()> {
    let path = PathBuf::from("/home/garrett/test-archive");
    let archive = Archive::open(&path)?;

    let entry = archive.get("null").unwrap();
    let handle = &entry.data.as_ref().unwrap();
    println!("{:?}", entry.metadata);
    println!("{:?}", handle);

    let mut buffer = Vec::new();
    let mut reader = archive.read(handle)?;
    reader.read_to_end(&mut buffer)?;

    println!("{:?}", &buffer);

    Ok(())
}

fn create() -> Result<()> {
    let path = PathBuf::from("/home/garrett/test-archive");
    let mut archive = Archive::create(&path)?;

    let mut entry = ArchiveObject::new();
    let handle = archive.write(&mut b"Garrett".as_ref())?;
    entry.metadata.insert(String::from("Tag"), b"Meta".to_vec());
    entry.data = Some(handle);

    archive.insert("null", entry);
    archive.commit()?;

    Ok(())
}
