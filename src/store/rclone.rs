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

#![cfg(feature = "store-rclone")]

use std::io;
use std::path::Path;
use std::process::{Command, Stdio};

use tempfile::TempDir;
use uuid::Uuid;

use crate::store::{DataStore, DirectoryStore, OpenOption, OpenStore};

/// Mount the rclone `remote` at `mount_directory`.
fn mount(remote: &str, mount_directory: &Path) -> io::Result<()> {
    let status = Command::new("rclone")
        .args(&[
            "mount",
            "--vfs-cache-mode",
            "writes",
            "--daemon",
            remote,
            mount_directory.to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()?;

    if status.success() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "Failed to mount rclone FUSE mount.",
        ))
    }
}

/// A `DataStore` which stores data using rclone.
///
/// This is a data store which is backed by [rclone](https://rclone.org/), allowing access to a wide
/// variety of cloud storage providers. This implementation uses FUSE, and currently only works on
/// Linux, macOS, and Windows using WSL2.
///
/// The `OpenStore::Config` value for this data store is a string with the format `<remote>:<path>`,
/// where `<remote>` is the name of the remote as configured using `rclone config` and `<path>` is
/// the path of the directory on the remote to use.
#[derive(Debug)]
pub struct RcloneStore {
    remote: String,
    mount_directory: TempDir,
    directory_store: DirectoryStore,
}

impl OpenStore for RcloneStore {
    type Config = String;

    fn open(config: Self::Config, options: OpenOption) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let mount_directory = tempfile::tempdir()?;
        mount(config.as_str(), mount_directory.as_ref())?;
        let directory_store =
            DirectoryStore::open(mount_directory.as_ref().to_path_buf(), options)?;

        Ok(RcloneStore {
            remote: config,
            mount_directory,
            directory_store,
        })
    }
}

impl DataStore for RcloneStore {
    type Error = io::Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        self.directory_store.write_block(id, data)
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        self.directory_store.read_block(id)
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        self.directory_store.remove_block(id)
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        self.directory_store.list_blocks()
    }
}
