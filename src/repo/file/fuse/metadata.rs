use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::HashMap;
use std::io;
use std::time::{Duration, SystemTime};

use fuse::{FileType as FuseFileType, Request};
use nix::libc;
use relative_path::RelativePath;
use time::Timespec;

use crate::repo::file::{
    Acl, AclMode, AclQualifier, AclType, Entry, EntryType, FileMode, FileRepo, UnixMetadata,
    UnixSpecial,
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
    let duration = Duration::new(time.sec.unsigned_abs(), time.nsec.unsigned_abs());
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
fn constrain_acl(acl: &mut HashMap<AclQualifier, AclMode>, mode: u32) {
    if let Some(acl_mode) = acl.get_mut(&AclQualifier::UserObj) {
        *acl_mode = AclMode::from_bits(acl_mode.bits() & user_perm(mode)).unwrap();
    }
    if let Some(acl_mode) = match acl.get_mut(&AclQualifier::Mask) {
        Some(acl_mode) => Some(acl_mode),
        None => acl.get_mut(&AclQualifier::GroupObj),
    } {
        *acl_mode = AclMode::from_bits(acl_mode.bits() & group_perm(mode)).unwrap();
    }
    if let Some(acl_mode) = acl.get_mut(&AclQualifier::Other) {
        *acl_mode = AclMode::from_bits(acl_mode.bits() & other_perm(mode)).unwrap();
    }
}

/// Return a new `mode` which does not exceed the permissions of the given `acl`.
fn constrain_mode(acl: &HashMap<AclQualifier, AclMode>, mode: u32) -> u32 {
    let mut acl_mode = 0u32;

    let user_mode = acl
        .get(&AclQualifier::UserObj)
        .copied()
        .unwrap_or(AclMode::RWX)
        .bits();
    acl_mode |= user_mode << 6;

    let group_mode = acl.get(&AclQualifier::GroupObj).copied();
    let mask_or_group_mode = acl
        .get(&AclQualifier::Mask)
        .copied()
        .or(group_mode)
        .unwrap_or(AclMode::RWX)
        .bits();
    acl_mode |= mask_or_group_mode << 3;

    let other_mode = acl
        .get(&AclQualifier::Other)
        .copied()
        .unwrap_or(AclMode::RWX)
        .bits();
    acl_mode |= other_mode;

    acl_mode & mode
}

impl UnixMetadata {
    /// Change the file mode and update the access ACLs accordingly.
    pub(super) fn change_permissions(&mut self, mode: u32) {
        self.mode = FileMode::from_bits_truncate(mode);

        // If we change the mode, we need to update the mandatory ACL entries to match.
        self.acl.access.remove(&AclQualifier::UserObj);
        self.acl.access.remove(&AclQualifier::Other);
        if !self.acl.access.contains_key(&AclQualifier::Mask) {
            // We only update the group permissions if there is no mask. Otherwise, we update
            // the mask permissions instead.
            self.acl.access.remove(&AclQualifier::GroupObj);
        }
        self.update_acl(AclType::ACCESS);

        // If we change the mode, and there is a mask entry in the ACL, we should use the group
        // permissions to set the mask.
        if let HashMapEntry::Occupied(mut mode_entry) = self.acl.access.entry(AclQualifier::Mask) {
            let group_mode = AclMode::from_bits(group_perm(self.mode.bits())).unwrap();
            mode_entry.insert(group_mode);
        }
    }
}

impl Entry<UnixSpecial, UnixMetadata> {
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
        parent: &Entry<UnixSpecial, UnixMetadata>,
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
                metadata.mode =
                    FileMode::from_bits_truncate(constrain_mode(&metadata.acl.access, mode));
            }
        }

        self
    }

    /// The default `UnixMetadata` for an entry that has no metadata.
    pub(super) fn default_metadata(&self, req: &Request) -> UnixMetadata {
        let now = SystemTime::now();
        UnixMetadata {
            mode: if self.is_directory() {
                FileMode::from_bits_truncate(DEFAULT_DIR_MODE)
            } else {
                FileMode::from_bits_truncate(DEFAULT_FILE_MODE)
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

impl EntryType<UnixSpecial> {
    /// Convert this `FileType` to a `fuse`-compatible file type.
    pub(super) fn to_file_type(&self) -> FuseFileType {
        match self {
            EntryType::File => FuseFileType::RegularFile,
            EntryType::Directory => FuseFileType::Directory,
            EntryType::Special(UnixSpecial::BlockDevice { .. }) => FuseFileType::BlockDevice,
            EntryType::Special(UnixSpecial::CharDevice { .. }) => FuseFileType::CharDevice,
            EntryType::Special(UnixSpecial::Symlink { .. }) => FuseFileType::Symlink,
            EntryType::Special(UnixSpecial::NamedPipe { .. }) => FuseFileType::NamedPipe,
        }
    }
}

impl FileRepo<UnixSpecial, UnixMetadata> {
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

    /// Update an entry's `ctime`.
    pub(super) fn touch_changed(
        &mut self,
        path: &RelativePath,
        req: &Request,
    ) -> crate::Result<()> {
        let mut metadata = self.entry(path)?.metadata_or_default(req);
        let now = SystemTime::now();
        metadata.changed = now;
        self.set_metadata(path, Some(metadata))
    }
}
