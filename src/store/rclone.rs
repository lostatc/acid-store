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

#![cfg(all(unix, feature = "store-rclone"))]

use std::fs::remove_dir;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use uuid::Uuid;

use crate::store::{DataStore, DirectoryStore, OpenOption, OpenStore};

/// The amount of time to wait between checking if the rclone remote is mounted.
const MOUNT_WAIT_TIME: Duration = Duration::from_millis(100);

/// Mount the rclone `remote` at `mount_directory` and return the mount process.
fn mount(remote: &str, mount_directory: &Path) -> io::Result<Child> {
    Command::new("rclone")
        .args(&[
            "mount",
            "--vfs-cache-mode",
            "writes",
            remote,
            mount_directory.to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
}

/// Return whether the given `path` is a mountpoint.
fn is_mountpoint(path: &Path) -> io::Result<bool> {
    let device_id = path.metadata()?.dev();
    let parent_device_id = path
        .parent()
        .expect("Invalid mountpoint.")
        .metadata()?
        .dev();
    Ok(device_id != parent_device_id)
}

/// A `DataStore` which stores data in cloud storage using rclone.
///
/// The `store-rclone` cargo feature is required to use this.
///
/// This is a data store which is backed by [rclone](https://rclone.org/), allowing access to a wide
/// variety of cloud storage providers. This implementation uses FUSE via the `rclone mount`
/// command, and currently only works on Linux, macOS, and Windows using WSL2.
///
/// To use this data store, rclone must be installed and available on the `PATH`.
///
/// The `OpenStore::Config` value for this data store is a string with the format `<remote>:<path>`,
/// where `<remote>` is the name of the remote as configured using `rclone config` and `<path>` is
/// the path of the directory on the remote to use.
#[derive(Debug)]
pub struct RcloneStore {
    mount_directory: PathBuf,
    mount_process: Child,
    directory_store: DirectoryStore,
}

impl OpenStore for RcloneStore {
    type Config = String;

    fn open(config: Self::Config, options: OpenOption) -> crate::Result<Self>
    where
        Self: Sized,
    {
        // Mount the rclone remote as a FUSE mount.
        let mount_directory = tempfile::tempdir()?.into_path();
        let mount_process = mount(config.as_str(), &mount_directory)?;

        // Wait for the remote to be mounted.
        while !is_mountpoint(&mount_directory)? {
            sleep(MOUNT_WAIT_TIME);
        }

        let directory_store = DirectoryStore::open(mount_directory.clone(), options)?;

        Ok(RcloneStore {
            mount_directory,
            mount_process,
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

impl Drop for RcloneStore {
    fn drop(&mut self) {
        // Attempt to unmount the remote by sending SIGTERM to the process.
        kill(
            Pid::from_raw(self.mount_process.id() as i32),
            Signal::SIGTERM,
        )
        .ok();

        // Attempt to remove the mount directory.
        remove_dir(&self.mount_directory).ok();
    }
}
