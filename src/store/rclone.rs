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
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::str::FromStr;

use path_slash::PathExt;
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::store::{DataStore, OpenOption, OpenStore};

/// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "3aa9a968-b8a3-4805-b242-3a412572ae71";

lazy_static! {
    static ref BLOCKS_DIRECTORY: &'static Path = Path::new("blocks");
    static ref STAGE_DIRECTORY: &'static Path = Path::new("stage");
    static ref VERSION_FILE: &'static Path = Path::new("version");
}

/// The configuration for an `RcloneStore`.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RcloneConfig {
    /// The name of the rclone remote.
    pub remote: String,

    /// The relative path of the directory in the remote.
    pub root: PathBuf,
}

/// A `DataStore` which stores data using `rclone`.
#[derive(Debug)]
pub struct RcloneStore {
    remote: String,
    root: PathBuf,
}

impl RcloneStore {
    fn remote_path(&self, path: &Path) -> String {
        format!("{}:{}", self.remote, self.root.join(path).to_slash_lossy())
    }

    /// Return the path where a block with the given `id` will be stored.
    fn block_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        BLOCKS_DIRECTORY.join(&hex[..2]).join(hex)
    }

    /// Return the path where a block with the given `id` will be staged.
    fn staging_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        STAGE_DIRECTORY.join(hex)
    }

    /// Write the given `data` to the file at `path`.
    fn write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let mut process = Command::new("rclone")
            .args(&["rcat", &self.remote_path(path)])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdin = process.stdin.as_mut().unwrap();
        stdin.write_all(data)?;
        stdin.flush()?;

        if process.wait()?.success() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "The rclone process did not complete successfully.",
            ))
        }
    }

    /// Read the file at `path` and return its data.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let output = Command::new("rclone")
            .args(&["cat", &self.remote_path(path)])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .output()?;

        if output.status.success() {
            Ok(output.stdout)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "The rclone process did not complete successfully.",
            ))
        }
    }

    /// Create a directory if one does not exist at `path`.
    fn create_directory(&self, path: &Path) -> io::Result<()> {
        let status = Command::new("rclone")
            .args(&["mkdir", &self.remote_path(path)])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()?;

        if status.success() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "The rclone process did not complete successfully.",
            ))
        }
    }

    /// Create a directory and its parents directories if one does not exist at `path`.
    fn create_directories(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            self.create_directories(parent)?;
        }
        self.create_directory(path)
    }

    /// Recursively delete the file or directory at `path`.
    fn delete(&self, path: &Path) -> io::Result<()> {
        let status = Command::new("rclone")
            .args(&["purge", &self.remote_path(path)])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()?;

        if status.success() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "The rclone process did not complete successfully.",
            ))
        }
    }

    /// Return the children of the given `directory`.
    fn list(&self, directory: &Path) -> io::Result<Vec<PathBuf>> {
        let output = Command::new("rclone")
            .args(&["lsf", &self.remote_path(directory)])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .output()?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(output.stdout.as_slice())
                .lines()
                .map(|file_name| directory.join(file_name))
                .collect())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "The rclone process did not complete successfully.",
            ))
        }
    }

    /// Move the file at `source` to `dest`.
    fn move_file(&self, source: &Path, dest: &Path) -> io::Result<()> {
        let status = Command::new("rclone")
            .args(&["move", &self.remote_path(source), &self.remote_path(dest)])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()?;

        if status.success() {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "The rclone process did not complete successfully.",
            ))
        }
    }

    /// Check if the given `path` exists.
    fn exists(&self, path: &Path) -> io::Result<bool> {
        if let Some(parent) = path.parent() {
            Ok(self.list(parent)?.contains(&path.to_path_buf()))
        } else {
            // The given `path` must be the root directory.
            Ok(true)
        }
    }
}

impl OpenStore for RcloneStore {
    type Config = RcloneConfig;

    fn open(config: Self::Config, options: OpenOption) -> crate::Result<Self>
    where
        Self: Sized,
    {
        assert!(!config.root.is_absolute());

        let data_store = RcloneStore {
            remote: config.remote,
            root: config.root,
        };
        let exists = data_store.exists(Path::new(""))?;

        if options.contains(OpenOption::CREATE_NEW) && exists {
            return Err(crate::Error::AlreadyExists);
        } else if options.intersects(OpenOption::CREATE_NEW | OpenOption::CREATE) && !exists {
            data_store.create_directories(Path::new(""))?;
            data_store.write(&VERSION_FILE, CURRENT_VERSION.as_bytes())?;
        } else {
            // Read the version ID file.
            let version_bytes = data_store.read(&VERSION_FILE)?;
            let version_id = String::from_utf8_lossy(version_bytes.as_slice());

            // Verify the version ID.
            if version_id != CURRENT_VERSION {
                return Err(crate::Error::UnsupportedFormat);
            }

            if options.contains(OpenOption::TRUNCATE) {
                data_store.delete(&BLOCKS_DIRECTORY)?;
            }
        }

        Ok(data_store)
    }
}

impl DataStore for RcloneStore {
    type Error = io::Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let staging_path = self.staging_path(id);
        let block_path = self.block_path(id);
        self.create_directories(staging_path.parent().unwrap())?;
        self.create_directories(block_path.parent().unwrap())?;

        // Write to a staging file and then atomically move it to its final destination.
        self.write(&staging_path, data)?;
        self.move_file(&staging_path, &block_path)?;

        // Remove any unused staging files.
        self.delete(&STAGE_DIRECTORY)?;

        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let block_path = self.block_path(id);

        if self.exists(&block_path)? {
            Ok(Some(self.read(&block_path)?))
        } else {
            Ok(None)
        }
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        self.delete(self.block_path(id).as_path())
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        let mut blocks = Vec::new();

        for parent_directory in self.list(&BLOCKS_DIRECTORY)? {
            for file in self.list(parent_directory.as_path())? {
                let block_id = Uuid::from_str(
                    file.file_name()
                        .unwrap()
                        .to_str()
                        .expect("Block file name is invalid."),
                )
                .expect("Block file name is invalid.");

                blocks.push(block_id)
            }
        }

        Ok(blocks)
    }
}
