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

use std::path::PathBuf;

use disk_archive::{Archive, ArchiveEntry};

fn main() {
    let path = PathBuf::from("/home/garrett/test-archive");
    let mut archive = Archive::create(&path).unwrap();

    let mut entry = ArchiveEntry::new();
    let handle = archive.write(&mut [0u8; 8].as_ref()).unwrap();
    entry.metadata.insert(String::from("tag"), vec![0u8; 4]);
    entry.data = Some(handle);

    archive.insert("null", entry);
}
