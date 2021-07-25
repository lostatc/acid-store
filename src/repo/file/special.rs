/*
 * Copyright 2019-2021 Wren Powell
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
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[cfg(all(any(unix, doc), feature = "file-metadata"))]
use {
    nix::sys::stat::{major, makedev, minor, mknod, Mode, SFlag},
    nix::unistd::mkfifo,
    std::path::PathBuf,
};
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
use {
    std::fs::read_link,
    std::os::unix::fs::{symlink, MetadataExt},
};

/// A special file type.
///
/// This trait can be implemented to customize how [`FileRepo`] handles special file types.
///
/// [`FileRepo`]: crate::repo::file::FileRepo
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
pub struct NoSpecial;

impl SpecialType for NoSpecial {
    fn from_file(_path: &Path) -> io::Result<Option<Self>> {
        Ok(None)
    }

    fn create_file(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

/// A `SpecialType` which supports special file types on unix systems.
///
/// If the current user does not have the necessary permissions to create a block/character device,
/// [`create_file`] will silently ignore the error and return `Ok`.
///
/// [`create_file`]: crate::repo::file::SpecialType::create_file
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "file-metadata"))))]
pub enum UnixSpecial {
    /// A symbolic link which points to `target`.
    Symlink { target: PathBuf },

    /// A named pipe (FIFO).
    NamedPipe,

    /// A block device identified by a `major` and `minor` device number.
    BlockDevice { major: u64, minor: u64 },

    /// A character device identified by a `major` and `minor` device number.
    CharDevice { major: u64, minor: u64 },
}

#[cfg(all(any(unix, doc), feature = "file-metadata"))]
impl SpecialType for UnixSpecial {
    fn from_file(path: &Path) -> io::Result<Option<Self>> {
        let metadata = path.symlink_metadata()?;
        let file_type = SFlag::from_bits(metadata.mode() & SFlag::S_IFMT.bits()).unwrap();
        let special_file = if file_type.contains(SFlag::S_IFLNK) {
            Some(UnixSpecial::Symlink {
                target: read_link(path)?,
            })
        } else if file_type.contains(SFlag::S_IFIFO) {
            Some(UnixSpecial::NamedPipe)
        } else if file_type.contains(SFlag::S_IFBLK) {
            Some(UnixSpecial::BlockDevice {
                major: major(metadata.rdev()),
                minor: minor(metadata.rdev()),
            })
        } else if file_type.contains(SFlag::S_IFCHR) {
            Some(UnixSpecial::CharDevice {
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
            UnixSpecial::Symlink { target } => symlink(target, path)?,
            UnixSpecial::NamedPipe => mkfifo(path, Mode::S_IRWXU)
                .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?,
            UnixSpecial::CharDevice { major, minor } => {
                match mknod(path, SFlag::S_IFCHR, Mode::S_IRWXU, makedev(*major, *minor)) {
                    Err(nix::Error::Sys(nix::errno::Errno::EPERM)) => (),
                    Err(error) => return Err(io::Error::new(io::ErrorKind::Other, error)),
                    _ => (),
                }
            }
            UnixSpecial::BlockDevice { major, minor } => {
                match mknod(path, SFlag::S_IFBLK, Mode::S_IRWXU, makedev(*major, *minor)) {
                    Err(nix::Error::Sys(nix::errno::Errno::EPERM)) => (),
                    Err(error) => return Err(io::Error::new(io::ErrorKind::Other, error)),
                    _ => (),
                }
            }
        };

        Ok(())
    }
}
