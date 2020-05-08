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

use std::io;
use std::path::{Path, PathBuf};
#[cfg(all(unix, feature = "file-metadata"))]
use {
    std::fs::read_link,
    std::os::unix::fs::{symlink, MetadataExt},
};

use serde::de::DeserializeOwned;
use serde::Serialize;
#[cfg(all(unix, feature = "file-metadata"))]
use {
    nix::sys::stat::{major, makedev, minor, mknod, Mode, SFlag},
    nix::unistd::mkfifo,
};

/// A special file type.
pub trait SpecialType: Serialize + DeserializeOwned {
    /// Create a new instance from the file in the file system at `path`.
    ///
    /// This returns `None` if the file type at `path` is not supported.
    fn from_file(path: &Path) -> io::Result<Option<Self>>;

    /// Create a new file of this type in the file system at `path`.
    fn create_file(&self, path: &Path) -> io::Result<()>;
}

/// A `SpecialType` which doesn't support any special file types.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub struct NoSpecialType;

impl SpecialType for NoSpecialType {
    fn from_file(_path: &Path) -> io::Result<Option<Self>> {
        Ok(None)
    }

    fn create_file(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

/// A `SpecialType` which supports special file types on unix systems.
#[cfg(all(unix, feature = "file-metadata"))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum UnixSpecialType {
    SymbolicLink { target: PathBuf },
    NamedPipe,
    BlockDevice { major: u64, minor: u64 },
    CharacterDevice { major: u64, minor: u64 },
}

impl SpecialType for UnixSpecialType {
    fn from_file(path: &Path) -> io::Result<Option<Self>> {
        let metadata = path.metadata()?;
        let file_type = SFlag::from_bits(metadata.mode()).unwrap();
        let special_file = if file_type.contains(SFlag::S_IFLNK) {
            Some(UnixSpecialType::SymbolicLink {
                target: read_link(path)?,
            })
        } else if file_type.contains(SFlag::S_IFIFO) {
            Some(UnixSpecialType::NamedPipe)
        } else if file_type.contains(SFlag::S_IFBLK) {
            Some(UnixSpecialType::BlockDevice {
                major: major(metadata.rdev()),
                minor: minor(metadata.rdev()),
            })
        } else if file_type.contains(SFlag::S_IFCHR) {
            Some(UnixSpecialType::CharacterDevice {
                major: major(metadata.rdev()),
                minor: minor(metadata.rdev()),
            })
        } else {
            None
        };

        Ok(special_file)
    }

    fn create_file(&self, path: &Path) -> io::Result<()> {
        match self {
            UnixSpecialType::SymbolicLink(target) => symlink(target, path),
            UnixSpecialType::NamedPipe => mkfifo(path, Mode::S_IRWXU),
            UnixSpecialType::CharacterDevice { major, minor } => {
                mknod(path, SFlag::S_IFCHR, Mode::S_IRWXU, makedev(*major, *minor))?
            }
            UnixSpecialType::BlockDevice { major, minor } => {
                mknod(path, SFlag::S_IFBLK, Mode::S_IRWXU, makedev(*major, *minor))?
            }
        };

        Ok(())
    }
}
