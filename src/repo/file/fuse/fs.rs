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

#![cfg(all(any(unix, doc), feature = "fuse-mount"))]

use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::{Duration, SystemTime};

use fuse::{
    FileAttr, Filesystem, ReplyAttr, ReplyData, ReplyEmpty, ReplyEntry, ReplyOpen, Request,
};
use nix::fcntl::OFlag;
use nix::libc;
use nix::sys::stat;
use relative_path::RelativePathBuf;
use time::Timespec;

use super::inode::InodeTable;

use crate::repo::common::IdTable;
use crate::repo::file::fuse::handle::HandleTable;
use crate::repo::file::{
    entry::{Entry, FileType},
    metadata::UnixMetadata,
    repository::{FileRepo, EMPTY_PARENT},
    special::UnixSpecialType,
};

/// The block size used to calculate `st_blocks`.
const BLOCK_SIZE: u64 = 512;

/// The default TTL value to use in FUSE replies.
///
/// Because the backing `FileRepo` can only be safely modified through the FUSE file system, while
/// it is mounted, we can set this to an arbitrarily large value.
const DEFAULT_TTL: Timespec = Timespec {
    sec: i64::MAX,
    nsec: i32::MAX,
};

/// The value of `st_rdev` value to use if the file is not a character or block device.
const NON_SPECIAL_RDEV: u32 = 0;

/// The default permissions bits for a directory.
const DEFAULT_DIR_MODE: u32 = 0o775;

/// The default permissions bits for a file.
const DEFAULT_FILE_MODE: u32 = 0o664;

/// The set of `open` flags which are not supported by this file system.
const UNSUPPORTED_OPEN_FLAGS: OFlag = OFlag::O_DIRECT | OFlag::O_TMPFILE;

impl crate::Error {
    /// Get the libc errno for this error.
    fn to_errno(&self) -> i32 {
        match self {
            crate::Error::AlreadyExists => libc::EEXIST,
            crate::Error::NotFound => libc::ENOENT,
            crate::Error::InvalidPath => libc::ENOENT,
            crate::Error::NotEmpty => libc::ENOTEMPTY,
            crate::Error::NotDirectory => libc::ENOTDIR,
            crate::Error::NotFile => libc::EISDIR,
            crate::Error::Io(error) => match error.raw_os_error() {
                Some(errno) => errno,
                None => libc::EIO,
            },
            _ => libc::EIO,
        }
    }
}

/// Convert the given `time` to a `SystemTime`.
fn to_system_time(time: Timespec) -> SystemTime {
    let duration = Duration::new(time.sec.abs() as u64, time.nsec.abs() as u32);
    if time.sec.is_positive() {
        SystemTime::UNIX_EPOCH + duration
    } else {
        SystemTime::UNIX_EPOCH - duration
    }
}

/// Convert the given `time` to a `Timespec`.
fn to_timespec(time: SystemTime) -> Timespec {
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

impl Entry<UnixSpecialType, UnixMetadata> {
    /// Create a new `Entry` of the given `file_type` with default metadata.
    fn new(file_type: FileType<UnixSpecialType>, req: &Request) -> Self {
        let mut entry = Self {
            file_type,
            metadata: None,
        };
        entry.metadata = Some(entry.default_metadata(req));
        entry
    }

    /// The default `UnixMetadata` for an entry that has no metadata.
    fn default_metadata(&self, req: &Request) -> UnixMetadata {
        UnixMetadata {
            mode: if self.is_directory() {
                DEFAULT_DIR_MODE
            } else {
                DEFAULT_FILE_MODE
            },
            modified: SystemTime::now(),
            accessed: SystemTime::now(),
            user: req.uid(),
            group: req.gid(),
            attributes: HashMap::new(),
            acl: HashMap::new(),
        }
    }
}

pub struct FuseAdapter<'a> {
    /// The repository which contains the virtual file system.
    repo: &'a mut FileRepo<UnixSpecialType, UnixMetadata>,

    /// A table for allocating inodes.
    inodes: InodeTable,

    /// A table for allocating file handles.
    handles: HandleTable,
}

impl<'a> FuseAdapter<'a> {
    /// Create a new `FuseAdapter` from the given `repo`.
    pub fn new(repo: &'a mut FileRepo<UnixSpecialType, UnixMetadata>) -> Self {
        let mut inodes = InodeTable::new();

        for (path, _) in repo.0.state().walk(&*EMPTY_PARENT).unwrap() {
            inodes.insert(path);
        }

        Self {
            repo,
            inodes,
            handles: IdTable::new(),
        }
    }

    /// Return the path of the entry with the given `name` and `parent_inode`.
    ///
    /// If there is no such entry, this returns `None`.
    fn child_path(&self, parent_inode: u64, name: &OsStr) -> Option<RelativePathBuf> {
        Some(
            self.inodes
                .path(parent_inode)?
                .join(name.to_string_lossy().as_ref()),
        )
    }

    /// Get the `FileAttr` for the `entry` with the given `inode`.
    fn entry_attr(
        &self,
        entry: &Entry<UnixSpecialType, UnixMetadata>,
        inode: u64,
        req: &Request,
    ) -> Option<FileAttr> {
        let entry_path = self.inodes.path(inode)?;
        let default_metadata = entry.default_metadata(req);
        let metadata = entry.metadata.as_ref().unwrap_or(&default_metadata);

        let size = match &entry.file_type {
            FileType::File => match self.repo.open(entry_path) {
                Ok(object) => object.size(),
                Err(crate::Error::NotFound) => return None,
                Err(_) => panic!("The entry is a file in the repository but could not be opened."),
            },
            FileType::Directory => 0,
            FileType::Special(special) => match special {
                // The `st_size` of a symlink should be the length of the pathname it contains.
                UnixSpecialType::SymbolicLink { target } => target.as_os_str().len() as u64,
                _ => 0,
            },
        };

        Some(FileAttr {
            ino: inode,
            size,
            blocks: size / BLOCK_SIZE,
            atime: to_timespec(metadata.accessed),
            mtime: to_timespec(metadata.modified),
            ctime: to_timespec(SystemTime::now()),
            crtime: to_timespec(SystemTime::now()),
            kind: match &entry.file_type {
                FileType::File => fuse::FileType::RegularFile,
                FileType::Directory => fuse::FileType::Directory,
                FileType::Special(special) => match special {
                    UnixSpecialType::SymbolicLink { .. } => fuse::FileType::Symlink,
                    UnixSpecialType::NamedPipe => fuse::FileType::NamedPipe,
                    UnixSpecialType::BlockDevice { .. } => fuse::FileType::BlockDevice,
                    UnixSpecialType::CharacterDevice { .. } => fuse::FileType::CharDevice,
                },
            },
            perm: metadata.mode as u16,
            nlink: 0,
            uid: metadata.user,
            gid: metadata.group,
            rdev: match &entry.file_type {
                FileType::Special(special) => match special {
                    UnixSpecialType::BlockDevice { major, minor } => {
                        stat::makedev(*major, *minor) as u32
                    }
                    UnixSpecialType::CharacterDevice { major, minor } => {
                        stat::makedev(*major, *minor) as u32
                    }
                    _ => NON_SPECIAL_RDEV,
                },
                _ => NON_SPECIAL_RDEV,
            },
            flags: 0,
        })
    }
}

impl<'a> Filesystem for FuseAdapter<'a> {
    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let entry_path = match self.child_path(parent, name) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let entry_inode = self.inodes.inode(&entry_path).unwrap();
        let entry = match self.repo.entry(&entry_path) {
            Ok(entry) => entry,
            Err(error) => {
                reply.error(error.to_errno());
                return;
            }
        };

        let attr = self.entry_attr(&entry, entry_inode, req).unwrap();
        let generation = self.inodes.generation(entry_inode);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        let entry_path = match self.inodes.path(ino) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let entry = match self.repo.entry(&entry_path) {
            Ok(entry) => entry,
            Err(error) => {
                reply.error(error.to_errno());
                return;
            }
        };
        let attr = self.entry_attr(&entry, ino, req).unwrap();

        reply.attr(&DEFAULT_TTL, &attr);
    }

    fn setattr(
        &mut self,
        req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        _size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        _chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let entry_path = match self.inodes.path(ino) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let mut entry = match self.repo.entry(&entry_path) {
            Ok(entry) => entry,
            Err(error) => {
                reply.error(error.to_errno());
                return;
            }
        };

        let default_metadata = entry.default_metadata(req);
        let metadata = entry.metadata.get_or_insert(default_metadata);

        if let Some(mode) = mode {
            metadata.mode = mode;
        }

        if let Some(uid) = uid {
            metadata.user = uid;
        }

        if let Some(gid) = gid {
            metadata.group = gid;
        }

        if let Some(atime) = atime {
            metadata.accessed = to_system_time(atime);
        }

        if let Some(mtime) = mtime {
            metadata.modified = to_system_time(mtime);
        }

        if let Err(error) = self.repo.set_metadata(entry_path, entry.metadata.clone()) {
            reply.error(error.to_errno());
            return;
        }

        let attr = self.entry_attr(&entry, ino, req).unwrap();
        reply.attr(&DEFAULT_TTL, &attr);
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        let entry_path = match self.inodes.path(ino) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let entry = match self.repo.entry(&entry_path) {
            Ok(entry) => entry,
            Err(error) => {
                reply.error(error.to_errno());
                return;
            }
        };
        match &entry.file_type {
            FileType::Special(UnixSpecialType::SymbolicLink { target }) => {
                reply.data(target.as_os_str().as_bytes());
            }
            _ => {
                reply.error(libc::EINVAL);
            }
        };
    }

    fn mknod(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        let entry_path = match self.child_path(parent, name) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let file_type = match stat::SFlag::from_bits(mode) {
            Some(s_flag) => {
                if s_flag.contains(stat::SFlag::S_IFREG) {
                    FileType::File
                } else if s_flag.contains(stat::SFlag::S_IFCHR) {
                    let major = stat::major(rdev as u64);
                    let minor = stat::minor(rdev as u64);
                    FileType::Special(UnixSpecialType::CharacterDevice { major, minor })
                } else if s_flag.contains(stat::SFlag::S_IFBLK) {
                    let major = stat::major(rdev as u64);
                    let minor = stat::minor(rdev as u64);
                    FileType::Special(UnixSpecialType::BlockDevice { major, minor })
                } else if s_flag.contains(stat::SFlag::S_IFIFO) {
                    FileType::Special(UnixSpecialType::NamedPipe)
                } else if s_flag.contains(stat::SFlag::S_IFSOCK) {
                    // Sockets aren't supported by `FileRepo`. `mknod(2)` specifies that `EPERM`
                    // should be returned if the file system doesn't support the type of node being
                    // requested.
                    reply.error(libc::EPERM);
                    return;
                } else {
                    // Other file types aren't supported by `mknod`.
                    reply.error(libc::EINVAL);
                    return;
                }
            }
            None => {
                // The file mode could not be parsed as a valid file type.
                reply.error(libc::EINVAL);
                return;
            }
        };

        let entry = Entry::new(file_type, req);

        if let Err(error) = self.repo.create(&entry_path, &entry) {
            reply.error(error.to_errno());
            return;
        }

        let entry_inode = self.inodes.insert(entry_path);
        let attr = self.entry_attr(&entry, entry_inode, req).unwrap();
        let generation = self.inodes.generation(entry_inode);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        let entry_path = match self.child_path(parent, name) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let mut entry = Entry::new(FileType::Directory, req);
        let metadata = entry.metadata.as_mut().unwrap();
        metadata.mode = mode;

        if let Err(error) = self.repo.create(&entry_path, &entry) {
            reply.error(error.to_errno());
            return;
        };

        let entry_inode = self.inodes.insert(entry_path);
        let attr = self.entry_attr(&entry, entry_inode, req).unwrap();
        let generation = self.inodes.generation(entry_inode);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let entry_path = match self.child_path(parent, name) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if self.repo.is_directory(&entry_path) {
            reply.error(libc::EISDIR);
            return;
        }

        if let Err(error) = self.repo.remove(&entry_path) {
            reply.error(error.to_errno());
            return;
        }

        let entry_inode = self.inodes.inode(&entry_path).unwrap();
        self.inodes.remove(entry_inode);

        reply.ok();
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let entry_path = match self.child_path(parent, name) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if !self.repo.is_directory(&entry_path) {
            reply.error(libc::ENOTDIR);
            return;
        }

        // `FileRepo::remove` method checks that the directory entry is empty.
        if let Err(error) = self.repo.remove(&entry_path) {
            reply.error(error.to_errno());
            return;
        }

        let entry_inode = self.inodes.inode(&entry_path).unwrap();
        self.inodes.remove(entry_inode);

        reply.ok();
    }

    fn symlink(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        link: &Path,
        reply: ReplyEntry,
    ) {
        let entry_path = match self.child_path(parent, name) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let entry = Entry::new(
            FileType::Special(UnixSpecialType::SymbolicLink {
                target: link.to_owned(),
            }),
            req,
        );

        if let Err(error) = self.repo.create(&entry_path, &entry) {
            reply.error(error.to_errno());
            return;
        };

        let entry_inode = self.inodes.insert(entry_path);
        let attr = self.entry_attr(&entry, entry_inode, req).unwrap();
        let generation = self.inodes.generation(entry_inode);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        let source_path = match self.child_path(parent, name) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let dest_path = match self.child_path(newparent, newname) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if !self.repo.exists(&source_path) {
            reply.error(libc::ENOENT);
            return;
        }

        // We cannot make a directory a subdirectory of itself.
        if dest_path.starts_with(&source_path) {
            reply.error(libc::EINVAL);
            return;
        }

        // Check if the parent of the destination path is not a directory.
        if !self.repo.is_directory(&dest_path.parent().unwrap()) {
            reply.error(libc::ENOTDIR);
            return;
        }

        // Remove the destination path unless it is a non-empty directory.
        if let Err(error @ crate::Error::NotEmpty) = self.repo.remove(&dest_path) {
            reply.error(error.to_errno());
            return;
        }

        // We've already checked all the possible error conditions.
        self.repo.copy(&source_path, &dest_path).ok();

        reply.ok();
    }

    fn link(
        &mut self,
        _req: &Request,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(libc::ENOSYS);
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        let flags = match OFlag::from_bits(flags as i32) {
            Some(flags) => flags,
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        if flags.intersects(UNSUPPORTED_OPEN_FLAGS) {
            reply.error(libc::ENOTSUP);
            return;
        }

        let entry_path = match self.inodes.path(ino) {
            Some(path) => path,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        if self.repo.is_directory(&entry_path) {
            reply.error(libc::EISDIR);
            return;
        }

        let fh = self.handles.open(flags);
        reply.opened(fh, 0);
    }
}
