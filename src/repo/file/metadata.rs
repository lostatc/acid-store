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

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io;
use std::path::Path;
#[cfg(feature = "file-metadata")]
use {filetime::set_file_times, std::time::SystemTime};
#[cfg(all(unix, feature = "file-metadata"))]
use {
    nix::sys::stat::Mode,
    nix::unistd::{chown, Gid, Uid},
    std::collections::HashMap,
    std::fs::set_permissions,
    std::os::unix::fs::{MetadataExt, PermissionsExt},
};

/// The metadata for a file in the file system.
///
/// This type must implement `Default` to provide the default metadata for a new entry.
pub trait FileMetadata: Default + Serialize + DeserializeOwned {
    /// Read the metadata from the file at `path` and create a new instance.
    fn from_file(path: &Path) -> io::Result<Self>;

    /// Write this metadata to the file at `path`.
    fn write_metadata(&self, path: &Path) -> io::Result<()>;
}

/// A `FileMetadata` which stores no metadata.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Default, Serialize, Deserialize)]
pub struct NoMetadata;

impl FileMetadata for NoMetadata {
    fn from_file(_path: &Path) -> io::Result<Self> {
        Ok(NoMetadata)
    }

    fn write_metadata(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }
}

/// A `FileMetadata` for unix-like operating systems.
///
/// If the current user does not have the necessary permissions to set the UID/GID of the file,
/// `write_metadata` will silently ignore the error and return `Ok`.
#[cfg(all(unix, feature = "file-metadata"))]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct UnixMetadata {
    /// The file mode (st_mode).
    pub mode: u32,

    /// The time the file was last modified (st_mtim).
    pub modified: SystemTime,

    /// The time the file was last accessed (st_atim).
    pub accessed: SystemTime,

    /// The UID of the user which owns the file (st_uid).
    pub user: u32,

    /// The GID of the group which owns the file (st_gid).
    pub group: u32,

    /// The extended attributes of the file.
    pub attributes: HashMap<String, Vec<u8>>,
}

#[cfg(all(unix, feature = "file-metadata"))]
impl FileMetadata for UnixMetadata {
    fn from_file(path: &Path) -> io::Result<Self> {
        let metadata = path.metadata()?;

        let mut attributes = HashMap::new();
        for attr_name in xattr::list(&path)? {
            if let Some(attr_value) = xattr::get(&path, &attr_name)? {
                attributes.insert(attr_name.to_string_lossy().to_string(), attr_value);
            }
        }

        Ok(Self {
            mode: metadata.mode(),
            modified: metadata.modified()?,
            accessed: metadata.accessed()?,
            user: metadata.uid(),
            group: metadata.gid(),
            attributes,
        })
    }

    fn write_metadata(&self, path: &Path) -> io::Result<()> {
        for (attr_name, attr_value) in self.attributes.iter() {
            xattr::set(&path, &attr_name, &attr_value)?;
        }
        set_permissions(path, PermissionsExt::from_mode(self.mode))?;
        match chown(
            path,
            Some(Uid::from_raw(self.user)),
            Some(Gid::from_raw(self.group)),
        ) {
            Err(nix::Error::Sys(nix::errno::Errno::EPERM)) => (),
            Err(error) => return Err(io::Error::new(io::ErrorKind::Other, error)),
            _ => (),
        };
        set_file_times(path, self.accessed.into(), self.modified.into())?;

        Ok(())
    }
}

#[cfg(all(unix, feature = "file-metadata"))]
impl Default for UnixMetadata {
    fn default() -> Self {
        Self {
            mode: (Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH).bits(),
            accessed: SystemTime::now(),
            modified: SystemTime::now(),
            user: Uid::current().as_raw(),
            group: Gid::current().as_raw(),
            attributes: HashMap::new(),
        }
    }
}

/// A `FileMetadata` for metadata that is common to most platforms.
#[cfg(feature = "file-metadata")]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct CommonMetadata {
    /// The time the file was last modified.
    pub modified: SystemTime,

    /// The time the file was last accessed.
    pub accessed: SystemTime,
}

#[cfg(feature = "file-metadata")]
impl FileMetadata for CommonMetadata {
    fn from_file(path: &Path) -> io::Result<Self> {
        let metadata = path.metadata()?;
        Ok(Self {
            modified: metadata.modified()?,
            accessed: metadata.accessed()?,
        })
    }

    fn write_metadata(&self, path: &Path) -> io::Result<()> {
        set_file_times(path, self.accessed.into(), self.modified.into())
    }
}

#[cfg(feature = "file-metadata")]
impl Default for CommonMetadata {
    fn default() -> Self {
        Self {
            modified: SystemTime::now(),
            accessed: SystemTime::now(),
        }
    }
}
