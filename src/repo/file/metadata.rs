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
    bitflags::bitflags,
    exacl::{AclEntry, AclEntryKind},
    nix::sys::stat,
    nix::unistd::{chown, Gid, Uid},
    std::collections::HashMap,
    std::fs::set_permissions,
    std::os::unix::fs::{MetadataExt, PermissionsExt},
    std::time::{Duration, UNIX_EPOCH},
    users::{get_group_by_name, get_user_by_name},
};
#[cfg(feature = "file-metadata")]
use {filetime::set_file_times, std::time::SystemTime};

/// The metadata for a file in the file system.
///
/// This trait can be implemented to customize how [`FileRepo`] handles file metadata.
///
/// [`FileRepo`]: crate::repo::file::FileRepo
pub trait FileMetadata: Serialize + DeserializeOwned {
    /// Read the metadata from the file at `path` and create a new instance.
    ///
    /// This returns `Ok(None)` if this implementation does not support reading file metadata.
    fn from_file(path: &Path) -> io::Result<Option<Self>>;

    /// Write this metadata to the file at `path`.
    fn write_metadata(&self, path: &Path) -> io::Result<()>;
}

/// A `FileMetadata` which stores no metadata.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum NoMetadata {}

impl FileMetadata for NoMetadata {
    fn from_file(_path: &Path) -> io::Result<Option<Self>> {
        Ok(None)
    }

    fn write_metadata(&self, _path: &Path) -> io::Result<()> {
        unreachable!("It is not possible to instantiate a `NoMetadata`.")
    }
}

/// A qualifier which determines who is granted a set of permissions in an ACL.
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "file-metadata"))))]
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum AclQualifier {
    /// The user with a given UID.
    User(u32),

    /// The group with a given GID.
    Group(u32),

    /// The user that owns the file.
    UserObj,

    /// The group that owns the file.
    GroupObj,

    /// Everyone else.
    Other,

    /// The ACL mask.
    Mask,
}

#[cfg(all(any(unix, doc), feature = "file-metadata"))]
bitflags! {
    /// The permission mode for an access control list.
    #[cfg_attr(docsrs, doc(cfg(all(unix, feature = "file-metadata"))))]
    #[derive(Serialize, Deserialize)]
    pub struct AclMode: u32 {
        /// Read permissions.
        const R = exacl::Perm::READ.bits();

        /// Write permissions.
        const W = exacl::Perm::WRITE.bits();

        /// Execute permissions.
        const X = exacl::Perm::EXECUTE.bits();

        /// Read, write, and execute permissions.
        const RWX = Self::R.bits | Self::W.bits | Self::X.bits;
    }

}

#[cfg(all(any(unix, doc), feature = "file-metadata"))]
bitflags! {
    /// A type of access control list.
    #[cfg_attr(docsrs, doc(cfg(all(unix, feature = "file-metadata"))))]
    #[derive(Serialize, Deserialize)]
    pub struct AclType: u32 {
        /// The access ACL.
        const ACCESS = 1 << 0;

        /// The default ACL.
        const DEFAULT = 1 << 1;
    }
}

/// The access control lists associated with a file.
///
/// This contains maps of qualifiers to their associated permissions.
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "file-metadata"))))]
#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct Acl {
    /// The access ACL.
    ///
    /// This ACL defines the current access permissions.
    pub access: HashMap<AclQualifier, AclMode>,

    /// The default ACL.
    ///
    /// This ACL defines the access permissions inherited by descendants. Setting this value only
    /// makes sense for directories.
    pub default: HashMap<AclQualifier, AclMode>,
}

#[cfg(all(any(unix, doc), feature = "file-metadata"))]
impl Acl {
    /// Construct a new empty `Acl`.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Construct a `SystemTime` from a unix timestamp.
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
fn unix_file_time(secs: i64, nsec: i64) -> SystemTime {
    let file_time = if secs.is_positive() {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(secs as u64)
    };
    if nsec.is_positive() {
        file_time + Duration::from_nanos(nsec as u64)
    } else {
        file_time - Duration::from_nanos(nsec as u64)
    }
}

/// Extract the user permission bits from a file `mode`.
fn user_perm(mode: u32) -> u32 {
    (mode & 0o700) >> 6
}

/// Extract the group permission bits from a file `mode`.
fn group_perm(mode: u32) -> u32 {
    (mode & 0o070) >> 3
}

/// Extract the other permission bits from a file `mode`.
fn other_perm(mode: u32) -> u32 {
    mode & 0o007
}

/// Calculate the `AclMode` for the ACL mask entry.
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
fn calculate_mask(acl: &HashMap<AclQualifier, AclMode>, mode: u32) -> AclMode {
    let mut mask_perm = acl
        .iter()
        .filter(|(qualifier, _)| {
            matches!(qualifier, AclQualifier::User(_) | AclQualifier::Group(_))
        })
        .map(|(_, access_mode)| access_mode)
        .fold(0u32, |accumulator, perm| accumulator | perm.bits());

    mask_perm |= acl
        .get(&AclQualifier::GroupObj)
        .map(|perm| perm.bits())
        .unwrap_or_else(|| group_perm(mode));

    AclMode::from_bits(mask_perm).unwrap()
}

/// Convert the given `entry` into an `AclQualifier`.
///
/// This returns `None` if it could not be converted.
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
fn entry_to_qualifier(entry: &AclEntry) -> Option<AclQualifier> {
    match entry.kind {
        AclEntryKind::User if entry.name.is_empty() => Some(AclQualifier::UserObj),
        AclEntryKind::User => match entry.name.parse().ok() {
            Some(uid) => Some(AclQualifier::User(uid)),
            None => get_user_by_name(&entry.name).map(|name| AclQualifier::User(name.uid())),
        },
        AclEntryKind::Group if entry.name.is_empty() => Some(AclQualifier::GroupObj),
        AclEntryKind::Group => match entry.name.parse().ok() {
            Some(gid) => Some(AclQualifier::Group(gid)),
            None => get_group_by_name(&entry.name).map(|group| AclQualifier::Group(group.gid())),
        },
        AclEntryKind::Other => Some(AclQualifier::Other),
        AclEntryKind::Mask => Some(AclQualifier::Mask),
        _ => None,
    }
}

/// Convert the given `qualifier`, `mode`, and `flags` into an `AclEntry`.
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
fn qualifier_to_entry(qualifier: AclQualifier, mode: AclMode, flags: exacl::Flag) -> AclEntry {
    let (kind, name) = match qualifier {
        AclQualifier::UserObj => (AclEntryKind::User, String::new()),
        AclQualifier::User(uid) => (AclEntryKind::User, uid.to_string()),
        AclQualifier::GroupObj => (AclEntryKind::Group, String::new()),
        AclQualifier::Group(gid) => (AclEntryKind::Group, gid.to_string()),
        AclQualifier::Other => (AclEntryKind::Other, String::new()),
        AclQualifier::Mask => (AclEntryKind::Mask, String::new()),
    };

    AclEntry {
        kind,
        name,
        perms: exacl::Perm::from_bits(mode.bits()).unwrap(),
        flags,
        allow: true,
    }
}

#[cfg(all(any(unix, doc), feature = "file-metadata"))]
bitflags! {
    /// The permission mode for a file.
    #[cfg_attr(docsrs, doc(cfg(all(unix, feature = "file-metadata"))))]
    #[derive(Serialize, Deserialize)]
    pub struct FileMode: u32 {
        /// Read permissions for the owning user (`0o400`).
        const S_IRUSR = stat::Mode::S_IRUSR.bits();

        /// Write permissions for the owning user (`0o200`).
        const S_IWUSR = stat::Mode::S_IWUSR.bits();

        /// Execute permissions for the owning user (`0o100`).
        const S_IXUSR = stat::Mode::S_IXUSR.bits();

        /// Read, write, and execute permissions for the owning user (`0o700`).
        const S_IRWXU = stat::Mode::S_IRWXU.bits();

        /// Read permissions for the owning group (`0o40`).
        const S_IRGRP = stat::Mode::S_IRGRP.bits();

        /// Write permissions for the owning group (`0o20`).
        const S_IWGRP = stat::Mode::S_IWGRP.bits();

        /// Execute permissions for the owning group (`0o10`).
        const S_IXGRP = stat::Mode::S_IXGRP.bits();

        /// Read, write, and execute permissions for the owning group (`0o70`).
        const S_IRWXG = stat::Mode::S_IRWXG.bits();

        /// Read permissions for everyone else (`0o4`).
        const S_IROTH = stat::Mode::S_IROTH.bits();

        /// Write permissions for everyone else (`0o2`).
        const S_IWOTH = stat::Mode::S_IWOTH.bits();

        /// Execute permissions for everyone else (`0o1`).
        const S_IXOTH = stat::Mode::S_IXOTH.bits();

        /// Read, write, and execute permissions for everyone else (`0o7`).
        const S_IRWXO = stat::Mode::S_IRWXO.bits();

        /// The set-user-ID on execute bit (`0o4000`).
        const S_ISUID = stat::Mode::S_ISUID.bits();

        /// The set-group-ID on execute bit (`0o2000`).
        const S_ISGID = stat::Mode::S_ISGID.bits();

        /// The sticky bit (`0o1000`).
        const S_ISVTX = stat::Mode::S_ISVTX.bits();
    }

}

/// A `FileMetadata` for unix-like operating systems.
///
/// Extended attributes and access control lists may not work on all platforms. If a platform is
/// unsupported, [`from_file`] will acts as if files have no extended attributes or ACL entries and
/// [`write_metadata`] will not attempt to write them.
///
/// If the current user does not have the necessary permissions to set the UID/GID of the file,
/// [`write_metadata`] will silently ignore the error and return `Ok`.
///
/// [`from_file`]: crate::repo::file::FileMetadata::from_file
/// [`write_metadata`]: crate::repo::file::FileMetadata::write_metadata
#[cfg(all(any(unix, doc), feature = "file-metadata"))]
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "file-metadata"))))]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct UnixMetadata {
    /// The file mode (st_mode).
    pub mode: FileMode,

    /// The time the file was last modified (st_mtime).
    pub modified: SystemTime,

    /// The time the file was last accessed (st_atime).
    pub accessed: SystemTime,

    /// The time the file metadata was last changed (st_ctime).
    pub changed: SystemTime,

    /// The UID of the user which owns the file (st_uid).
    pub user: u32,

    /// The GID of the group which owns the file (st_gid).
    pub group: u32,

    /// The extended attributes of the file.
    pub attributes: HashMap<String, Vec<u8>>,

    /// The access control lists for the file.
    ///
    /// When [`FileMetadata::write_metadata`] is called, any missing mandatory ACL entries are
    /// calculated automatically using [`update_acl`].
    ///
    /// [`FileMetadata::write_metadata`]: crate::repo::file::FileMetadata::write_metadata
    /// [`update_acl`]: crate::repo::file::UnixMetadata::update_acl
    pub acl: Acl,
}

#[cfg(all(any(unix, doc), feature = "file-metadata"))]
impl UnixMetadata {
    /// Add any missing mandatory ACL entries to [`acl`].
    ///
    /// If [`AclQualifier::UserObj`], [`AclQualifier::GroupObj`], or
    /// [`AclQualifier::Other`] are missing from [`acl`], this method calculates them from the
    /// [`mode`] and inserts them into the map. A `kind` can be passed which determines whether
    /// this affects the access ACL, the default ACL, or both.
    ///
    /// [`AclQualifier::UserObj`]: crate::repo::file::AclQualifier::UserObj
    /// [`AclQualifier::GroupObj`]: crate::repo::file::AclQualifier::GroupObj
    /// [`AclQualifier::Other`]: crate::repo::file::AclQualifier::Other
    /// [`acl`]: crate::repo::file::UnixMetadata::acl
    /// [`mode`]: crate::repo::file::UnixMetadata::mode
    pub fn update_acl(&mut self, kind: AclType) {
        let user_perm = user_perm(self.mode.bits());
        let group_perm = group_perm(self.mode.bits());
        let other_perm = other_perm(self.mode.bits());

        if kind.contains(AclType::ACCESS) {
            self.acl
                .access
                .entry(AclQualifier::UserObj)
                .or_insert_with(|| AclMode::from_bits(user_perm).unwrap());
            self.acl
                .access
                .entry(AclQualifier::GroupObj)
                .or_insert_with(|| AclMode::from_bits(group_perm).unwrap());
            self.acl
                .access
                .entry(AclQualifier::Other)
                .or_insert_with(|| AclMode::from_bits(other_perm).unwrap());
        }

        if kind.contains(AclType::DEFAULT) {
            self.acl
                .default
                .entry(AclQualifier::UserObj)
                .or_insert_with(|| AclMode::from_bits(user_perm).unwrap());
            self.acl
                .default
                .entry(AclQualifier::GroupObj)
                .or_insert_with(|| AclMode::from_bits(group_perm).unwrap());
            self.acl
                .default
                .entry(AclQualifier::Other)
                .or_insert_with(|| AclMode::from_bits(other_perm).unwrap());
        }
    }

    /// Recalculate the ACL mask entry.
    ///
    /// This method calculates and adds an [`AclQualifier::Mask`] entry, replacing the existing
    /// one if one already exists. A `kind` can be passed which determines whether this affects the
    /// access ACL, the default ACL, or both.
    ///
    /// [`AclQualifier::Mask`]: crate::repo::file::AclQualifier::Mask
    pub fn update_mask(&mut self, kind: AclType) {
        if kind.contains(AclType::ACCESS) {
            self.acl.access.insert(
                AclQualifier::Mask,
                calculate_mask(&self.acl.access, self.mode.bits()),
            );
        }

        if kind.contains(AclType::DEFAULT) {
            self.acl.default.insert(
                AclQualifier::Mask,
                calculate_mask(&self.acl.default, self.mode.bits()),
            );
        }
    }
}

#[cfg(all(any(unix, doc), feature = "file-metadata"))]
impl FileMetadata for UnixMetadata {
    fn from_file(path: &Path) -> io::Result<Option<Self>> {
        let metadata = path.metadata()?;

        let mut attributes = HashMap::new();
        if xattr::SUPPORTED_PLATFORM {
            for attr_name in xattr::list(&path)? {
                if let Some(attr_value) = xattr::get(&path, &attr_name)? {
                    attributes.insert(attr_name.to_string_lossy().to_string(), attr_value);
                }
            }
        }

        let mut mode = metadata.mode();

        #[cfg(not(target_os = "linux"))]
        let acl = HashMap::new();

        // We only support ACLs on Linux currently.
        #[cfg(target_os = "linux")]
        let acl = {
            let acl_entries = exacl::getfacl(path, exacl::AclOption::empty())?;

            // Calculate the owner permissions stored in the ACL so we can ensure it stays in sync
            // with the file mode.
            let mut acl_mode = 0;
            for entry in &acl_entries {
                if entry.flags.contains(exacl::Flag::DEFAULT) {
                    continue;
                }

                // If the `AclEntry.name` of a user or group entry is empty, that means it
                // represents the file owner.
                match entry.kind {
                    AclEntryKind::User if entry.name.is_empty() => {
                        acl_mode |= entry.perms.bits() << 6;
                    }
                    AclEntryKind::Group if entry.name.is_empty() => {
                        acl_mode |= entry.perms.bits() << 3;
                    }
                    AclEntryKind::Other => {
                        acl_mode |= entry.perms.bits();
                    }
                    _ => {}
                }
            }

            // We want to replace the rwx bits and keep the rest of the bits unchanged.
            mode = (mode & !0o777) | (acl_mode & 0o777);

            let mut acl = Acl::new();
            for entry in acl_entries {
                let qualifier = match entry_to_qualifier(&entry) {
                    Some(qualifier) => qualifier,
                    None => continue,
                };
                let mode = AclMode::from_bits(entry.perms.bits()).unwrap();

                if entry.flags.contains(exacl::Flag::DEFAULT) {
                    acl.default.insert(qualifier, mode);
                } else {
                    acl.access.insert(qualifier, mode);
                }
            }

            acl
        };

        Ok(Some(Self {
            mode: FileMode::from_bits_truncate(mode),
            modified: unix_file_time(metadata.mtime(), metadata.mtime_nsec()),
            accessed: unix_file_time(metadata.atime(), metadata.atime_nsec()),
            changed: unix_file_time(metadata.ctime(), metadata.ctime_nsec()),
            user: metadata.uid(),
            group: metadata.gid(),
            attributes,
            acl,
        }))
    }

    fn write_metadata(&self, path: &Path) -> io::Result<()> {
        // The order we do these in is important, because the mode, extended attributes, and ACLs
        // all interact when it comes to file permissions. ACLs are technically stored as xattrs,
        // and ACLs contain information which is redundant with the file mode. We want to set the
        // file mode first so that any ACL information in the xattrs overwrites it. We want to set
        // the ACLs after setting the xattrs so that ACL entries supplied in `UnixMetadata.acl`
        // overwrites any information in the xattrs.

        set_permissions(path, PermissionsExt::from_mode(self.mode.bits()))?;

        if xattr::SUPPORTED_PLATFORM {
            for (attr_name, attr_value) in self.attributes.iter() {
                xattr::set(&path, &attr_name, &attr_value)?;
            }
        }

        // We only support ACLs on Linux currently.
        #[cfg(target_os = "linux")]
        {
            if !self.acl.access.is_empty() {
                // Add the necessary entries if they are missing, computing them from the `mode`.
                let mut metadata = self.clone();
                metadata.update_acl(AclType::ACCESS);

                let acl_entries = metadata
                    .acl
                    .access
                    .iter()
                    .map(|(qualifier, mode)| {
                        qualifier_to_entry(*qualifier, *mode, exacl::Flag::empty())
                    })
                    .collect::<Vec<_>>();

                exacl::setfacl(&[path], &acl_entries, exacl::AclOption::ACCESS_ACL)?;
            }

            if !self.acl.default.is_empty() {
                // Add the necessary entries if they are missing, computing them from the `mode`.
                let mut metadata = self.clone();
                metadata.update_acl(AclType::DEFAULT);

                let acl_entries = metadata
                    .acl
                    .default
                    .iter()
                    .map(|(qualifier, mode)| {
                        qualifier_to_entry(*qualifier, *mode, exacl::Flag::DEFAULT)
                    })
                    .collect::<Vec<_>>();

                exacl::setfacl(&[path], &acl_entries, exacl::AclOption::DEFAULT_ACL)?;
            }
        }

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

/// A `FileMetadata` for metadata that is common to most platforms.
#[cfg(feature = "file-metadata")]
#[cfg_attr(docsrs, doc(cfg(feature = "file-metadata")))]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct CommonMetadata {
    /// The time the file was last modified.
    pub modified: SystemTime,

    /// The time the file was last accessed.
    pub accessed: SystemTime,
}

#[cfg(feature = "file-metadata")]
impl FileMetadata for CommonMetadata {
    fn from_file(path: &Path) -> io::Result<Option<Self>> {
        let metadata = path.metadata()?;
        Ok(Some(Self {
            modified: metadata.modified()?,
            accessed: metadata.accessed()?,
        }))
    }

    fn write_metadata(&self, path: &Path) -> io::Result<()> {
        set_file_times(path, self.accessed.into(), self.modified.into())
    }
}
