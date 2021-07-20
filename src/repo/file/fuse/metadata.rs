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

use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::io;
use std::time::{Duration, SystemTime};

use fuse::{FileType as FuseFileType, Request};
use nix::libc;
use relative_path::RelativePath;
use time::Timespec;

use crate::repo::file::{
    AccessMode, AccessQualifier, Acl, AclType, Entry, EntryType, FileRepo, UnixMetadata,
    UnixSpecialType,
};

/// The default permissions bits for a directory.
const DEFAULT_DIR_MODE: u32 = 0o775;

/// The default permissions bits for a file.
const DEFAULT_FILE_MODE: u32 = 0o664;

impl crate::Error {
    /// Get the libc errno for this error.
    pub(super) fn to_errno(&self) -> i32 {
        match self {
            crate::Error::AlreadyExists => libc::EEXIST,
            crate::Error::NotFound => libc::ENOENT,
            crate::Error::InvalidPath => libc::ENOENT,
            crate::Error::NotEmpty => libc::ENOTEMPTY,
            crate::Error::NotDirectory => libc::ENOTDIR,
            crate::Error::NotFile => libc::EISDIR,
            crate::Error::Io(error) => match error.raw_os_error() {
                Some(errno) => errno,
                // Some third-party libraries use `std::io::Error` without there being an underlying
                // `Error::raw_os_error`.
                None => match error.kind() {
                    io::ErrorKind::NotFound => libc::ENOENT,
                    io::ErrorKind::PermissionDenied => libc::EPERM,
                    io::ErrorKind::ConnectionRefused => libc::ECONNREFUSED,
                    io::ErrorKind::ConnectionReset => libc::ECONNRESET,
                    io::ErrorKind::ConnectionAborted => libc::ECONNABORTED,
                    io::ErrorKind::NotConnected => libc::ENOTCONN,
                    io::ErrorKind::AddrInUse => libc::EADDRINUSE,
                    io::ErrorKind::AddrNotAvailable => libc::EADDRNOTAVAIL,
                    io::ErrorKind::BrokenPipe => libc::EPIPE,
                    io::ErrorKind::AlreadyExists => libc::EEXIST,
                    io::ErrorKind::WouldBlock => libc::EWOULDBLOCK,
                    io::ErrorKind::InvalidInput => libc::EINVAL,
                    io::ErrorKind::TimedOut => libc::ETIMEDOUT,
                    io::ErrorKind::Interrupted => libc::EINTR,
                    io::ErrorKind::Unsupported => libc::ENOSYS,
                    _ => libc::EIO,
                },
            },
            _ => libc::EIO,
        }
    }
}

/// Extract the user permission bits from a file `mode`.
pub fn user_perm(mode: u32) -> u32 {
    (mode & 0o700) >> 6
}

/// Extract the group permission bits from a file `mode`.
pub fn group_perm(mode: u32) -> u32 {
    (mode & 0o070) >> 3
}

/// Extract the other permission bits from a file `mode`.
pub fn other_perm(mode: u32) -> u32 {
    mode & 0o007
}

/// Convert the given `time` to a `SystemTime`.
pub fn to_system_time(time: Timespec) -> SystemTime {
    let duration = Duration::new(time.sec.abs() as u64, time.nsec.abs() as u32);
    if time.sec.is_positive() {
        SystemTime::UNIX_EPOCH + duration
    } else {
        SystemTime::UNIX_EPOCH - duration
    }
}

/// Convert the given `time` to a `Timespec`.
pub fn to_timespec(time: SystemTime) -> Timespec {
    match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => Timespec {
            sec: duration.as_secs() as i64,
            nsec: duration.subsec_nanos() as i32,
        },
        Err(error) => Timespec {
            sec: -(error.duration().as_secs() as i64),
            nsec: -(error.duration().subsec_nanos() as i32),
        },
    }
}

/// Modify the given `acl` so its permissions do not exceed the given `mode`.
///
/// This modifies the entries in `acl` which correspond to permissions in the file mode so that they
/// do not exceed the permissions granted by the given `mode`.
fn constrain_acl(acl: &mut HashMap<AccessQualifier, AccessMode>, mode: u32) {
    if let Some(acl_mode) = acl.get_mut(&AccessQualifier::UserObj) {
        *acl_mode = AccessMode::from_bits(acl_mode.bits() & user_perm(mode)).unwrap();
    }
    if let Some(acl_mode) = match acl.get_mut(&AccessQualifier::Mask) {
        Some(acl_mode) => Some(acl_mode),
        None => acl.get_mut(&AccessQualifier::GroupObj),
    } {
        *acl_mode = AccessMode::from_bits(acl_mode.bits() & group_perm(mode)).unwrap();
    }
    if let Some(acl_mode) = acl.get_mut(&AccessQualifier::Other) {
        *acl_mode = AccessMode::from_bits(acl_mode.bits() & other_perm(mode)).unwrap();
    }
}

/// Return a new `mode` which does not exceed the permissions of the given `acl`.
fn constrain_mode(acl: &HashMap<AccessQualifier, AccessMode>, mode: u32) -> u32 {
    let mut acl_mode = 0u32;

    let user_mode = acl
        .get(&AccessQualifier::UserObj)
        .copied()
        .unwrap_or(AccessMode::READ | AccessMode::WRITE | AccessMode::EXECUTE)
        .bits();
    acl_mode |= user_mode << 6;

    let group_mode = acl.get(&AccessQualifier::GroupObj).copied();
    let mask_or_group_mode = acl
        .get(&AccessQualifier::Mask)
        .copied()
        .or(group_mode)
        .unwrap_or(AccessMode::READ | AccessMode::WRITE | AccessMode::EXECUTE)
        .bits();
    acl_mode |= mask_or_group_mode << 3;

    let other_mode = acl
        .get(&AccessQualifier::Other)
        .copied()
        .unwrap_or(AccessMode::READ | AccessMode::WRITE | AccessMode::EXECUTE)
        .bits();
    acl_mode |= other_mode;

    acl_mode & mode
}

impl UnixMetadata {
    /// Change the file mode and update the access ACLs accordingly.
    pub(super) fn change_permissions(&mut self, mode: u32) {
        self.mode = mode;

        // If we change the mode, we need to update the mandatory ACL entries to match.
        self.acl.access.remove(&AccessQualifier::UserObj);
        self.acl.access.remove(&AccessQualifier::Other);
        if !self.acl.access.contains_key(&AccessQualifier::Mask) {
            // We only update the group permissions if there is no mask. Otherwise, we update
            // the mask permissions instead.
            self.acl.access.remove(&AccessQualifier::GroupObj);
        }
        self.update_acl(AclType::ACCESS);

        // If we change the mode, and there is a mask entry in the ACL, we should use the group
        // permissions to set the mask.
        if let HashMapEntry::Occupied(mut mode_entry) = self.acl.access.entry(AccessQualifier::Mask)
        {
            let group_mode = AccessMode::from_bits(group_perm(self.mode)).unwrap();
            mode_entry.insert(group_mode);
        }
    }
}

impl Entry<UnixSpecialType, UnixMetadata> {
    /// Set the metadata of this entry to the default metadata for a new entry.
    pub(super) fn with_metadata(mut self, req: &Request) -> Self {
        self.metadata = Some(self.default_metadata(req));
        self
    }

    /// Set the access ACL and mode for a new entry with the given `parent` and `mode`.
    ///
    /// This calculates the appropriate access ACL and mode for a new entry based on the `parent`
    /// default ACL and the given `mode`. If `mode` is `None`, this method does not set the mode.
    ///
    /// If this entry has no metadata, this does nothing.
    pub(super) fn with_permissions(
        mut self,
        parent: &Entry<UnixSpecialType, UnixMetadata>,
        mode: Option<u32>,
    ) -> Self {
        let is_directory = self.is_directory();
        if let (Some(metadata), Some(parent_metadata)) = (&mut self.metadata, &parent.metadata) {
            metadata.acl.access = parent_metadata.acl.default.clone();
            if is_directory {
                metadata.acl.default = parent_metadata.acl.default.clone();
            }

            if let Some(mode) = mode {
                constrain_acl(&mut metadata.acl.access, mode);
                metadata.mode = constrain_mode(&metadata.acl.access, mode);
            }
        }

        self
    }

    /// The default `UnixMetadata` for an entry that has no metadata.
    pub(super) fn default_metadata(&self, req: &Request) -> UnixMetadata {
        let now = SystemTime::now();
        UnixMetadata {
            mode: if self.is_directory() {
                DEFAULT_DIR_MODE
            } else {
                DEFAULT_FILE_MODE
            },
            modified: now,
            accessed: now,
            changed: now,
            user: req.uid(),
            group: req.gid(),
            attributes: HashMap::new(),
            acl: Acl::new(),
        }
    }

    /// Return this entry's metadata or the default metadata if it's `None`.
    pub(super) fn metadata_or_default(self, req: &Request) -> UnixMetadata {
        match self.metadata {
            Some(metadata) => metadata,
            None => self.default_metadata(req),
        }
    }
}

impl EntryType<UnixSpecialType> {
    /// Convert this `FileType` to a `fuse`-compatible file type.
    pub(super) fn to_file_type(&self) -> FuseFileType {
        match self {
            EntryType::File => FuseFileType::RegularFile,
            EntryType::Directory => FuseFileType::Directory,
            EntryType::Special(UnixSpecialType::BlockDevice { .. }) => FuseFileType::BlockDevice,
            EntryType::Special(UnixSpecialType::CharacterDevice { .. }) => FuseFileType::CharDevice,
            EntryType::Special(UnixSpecialType::SymbolicLink { .. }) => FuseFileType::Symlink,
            EntryType::Special(UnixSpecialType::NamedPipe { .. }) => FuseFileType::NamedPipe,
        }
    }
}

impl FileRepo<UnixSpecialType, UnixMetadata> {
    /// Update an entry's `mtime`, `atime`, and `ctime`.
    pub(super) fn touch_modified(
        &mut self,
        path: &RelativePath,
        req: &Request,
    ) -> crate::Result<()> {
        let mut metadata = self.entry(path)?.metadata_or_default(req);
        let now = SystemTime::now();
        metadata.modified = now;
        metadata.accessed = now;
        metadata.changed = now;
        self.set_metadata(path, Some(metadata))
    }

    /// Update an entry's `atime` and `ctime`.
    pub(super) fn touch_accessed(
        &mut self,
        path: &RelativePath,
        req: &Request,
    ) -> crate::Result<()> {
        let mut metadata = self.entry(path)?.metadata_or_default(req);
        let now = SystemTime::now();
        metadata.accessed = now;
        metadata.changed = now;
        self.set_metadata(path, Some(metadata))
    }
}
