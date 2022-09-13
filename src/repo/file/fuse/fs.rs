use std::collections::hash_map::Entry as HashMapEntry;
use std::ffi::OsStr;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::SystemTime;

use fuse::{
    FileAttr, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite, ReplyXattr, Request,
};
use nix::fcntl::OFlag;
use nix::libc;
use nix::sys::stat::{self, SFlag};
use once_cell::sync::Lazy;
use relative_path::{RelativePath, RelativePathBuf};
use time::Timespec;

use super::acl::{Permissions, ACCESS_ACL_XATTR, DEFAULT_ACL_XATTR};
use super::handle::{DirectoryEntry, DirectoryHandle, FileHandle, HandleState, HandleTable};
use super::inode::InodeTable;
use super::metadata::to_timespec;
use super::object::ObjectTable;

use crate::repo::file::fuse::metadata::to_system_time;
use crate::repo::file::{
    repository::EMPTY_PATH, AclQualifier, Entry, EntryType, FileMode, FileRepo, UnixMetadata,
    UnixSpecial, WalkPredicate,
};
use crate::repo::{Commit, RestoreSavepoint};

/// The block size used to calculate `st_blocks`.
const BLOCK_SIZE: u64 = 512;

/// The default TTL value to use in FUSE replies.
///
/// Because the backing `FileRepo` can only be safely modified through the FUSE file system, we can
/// set this to an arbitrarily large value.
const DEFAULT_TTL: Timespec = Timespec {
    sec: i64::MAX,
    nsec: i32::MAX,
};

/// The set of `open` flags which are not supported by this file system.
static UNSUPPORTED_OPEN_FLAGS: Lazy<OFlag> = Lazy::new(|| OFlag::O_DIRECT | OFlag::O_TMPFILE);

/// The value of `st_rdev` value to use if the file is not a character or block device.
const NON_SPECIAL_RDEV: u32 = 0;

/// Handle a `crate::Result` in a FUSE method.
macro_rules! try_result {
    ($result:expr, $reply:expr) => {
        match $result {
            Ok(result) => result,
            Err(error) => {
                $reply.error(crate::Error::from(error).to_errno());
                return;
            }
        }
    };
}

/// Handle an `Option` in a FUSE method.
macro_rules! try_option {
    ($result:expr, $reply:expr, $error:expr) => {
        match $result {
            Some(result) => result,
            None => {
                $reply.error($error);
                return;
            }
        }
    };
}

// A note about atomicity:
//
// Each implemented method of `Filesystem` should be atomic, meaning it only modifies the file
// system state when it returns successfully. If a method return an error, the file system should
// be unchanged. To accomplish this, this implementation follows the following pattern:
//
// 1. Modifications to the backing `FileRepo` should happen as an atomic transaction using the
// savepoint API. If the transaction fails, the repository should be restored to a savepoint and the
// method should return an error.
// 2. The file system state should not be modified before this transaction occurs.
// 3. All modifications to the file system state after the transaction completes successfully should
// be infallible.
//
// TODO: Refactor this module to enforce this pattern.

/// An adapter for implementing a FUSE file system backed by a `FileRepo`.
#[derive(Debug)]
pub struct FuseAdapter<'a> {
    /// The repository which contains the virtual file system.
    repo: &'a mut FileRepo<UnixSpecial, UnixMetadata>,

    /// A table for allocating inodes.
    inodes: InodeTable,

    /// A table for allocating file handles.
    handles: HandleTable,

    /// A map of inodes to currently open file objects.
    objects: ObjectTable,
}

impl<'a> FuseAdapter<'a> {
    /// Create a new `FuseAdapter` from the given `repo`.
    pub fn new(
        repo: &'a mut FileRepo<UnixSpecial, UnixMetadata>,
        root: &RelativePath,
    ) -> crate::Result<Self> {
        if root == *EMPTY_PATH {
            return Err(crate::Error::InvalidPath);
        }

        let mut inodes = InodeTable::new(root);

        repo.walk::<(), _, _>(root, |entry| {
            let entry_id = entry.entry_id();
            inodes.insert(entry.into_path(), entry_id);
            WalkPredicate::Continue
        })?;

        Ok(Self {
            repo,
            inodes,
            handles: HandleTable::new(),
            objects: ObjectTable::new(),
        })
    }

    /// Get the `FileAttr` for the `entry` with the given `inode`.
    fn entry_attr(
        &mut self,
        entry: &Entry<UnixSpecial, UnixMetadata>,
        inode: u64,
        req: &Request,
    ) -> crate::Result<FileAttr> {
        let entry_path = self.inodes.path(inode).ok_or(crate::Error::NotFound)?;
        let entry_id = self.repo.entry_id(entry_path)?;
        let default_metadata = entry.default_metadata(req);
        let metadata = entry.metadata.as_ref().unwrap_or(&default_metadata);

        let size = match &entry.kind {
            EntryType::File => self
                .objects
                .open_commit(inode, self.repo.open(entry_path).unwrap())?
                .size()
                .unwrap(),
            EntryType::Directory => 0,
            EntryType::Special(special) => match special {
                // The `st_size` of a symlink should be the length of the pathname it contains.
                UnixSpecial::Symlink { target } => target.as_os_str().len() as u64,
                _ => 0,
            },
        };

        // The mode returned needs to take into account the ACL mask if it is set, because it
        // affects the group permissions.
        let mode = match metadata.acl.access.get(&AclQualifier::Mask) {
            None => metadata.mode.bits(),
            Some(mask_mode) => (metadata.mode.bits() & 0o707) | (mask_mode.bits() << 3),
        };

        Ok(FileAttr {
            ino: inode,
            size,
            blocks: size / BLOCK_SIZE,
            atime: to_timespec(metadata.accessed),
            mtime: to_timespec(metadata.modified),
            ctime: to_timespec(metadata.changed),
            crtime: to_timespec(SystemTime::now()),
            kind: match &entry.kind {
                EntryType::File => fuse::FileType::RegularFile,
                EntryType::Directory => fuse::FileType::Directory,
                EntryType::Special(special) => match special {
                    UnixSpecial::Symlink { .. } => fuse::FileType::Symlink,
                    UnixSpecial::NamedPipe => fuse::FileType::NamedPipe,
                    UnixSpecial::BlockDevice { .. } => fuse::FileType::BlockDevice,
                    UnixSpecial::CharDevice { .. } => fuse::FileType::CharDevice,
                },
            },
            perm: mode as u16,
            nlink: self.repo.link_count(entry_id),
            uid: metadata.user,
            gid: metadata.group,
            rdev: match &entry.kind {
                EntryType::Special(special) => match special {
                    UnixSpecial::BlockDevice { major, minor } => {
                        stat::makedev(*major, *minor) as u32
                    }
                    UnixSpecial::CharDevice { major, minor } => {
                        stat::makedev(*major, *minor) as u32
                    }
                    _ => NON_SPECIAL_RDEV,
                },
                _ => NON_SPECIAL_RDEV,
            },
            flags: 0,
        })
    }

    /// Get the `FileAttr` for the `entry` at the given `path`.
    ///
    /// This also allocates an inode in the inode table for the entry. If this returns `Err`, the
    /// inode table is unchanged.
    pub fn create_attr(
        &mut self,
        path: RelativePathBuf,
        entry: &Entry<UnixSpecial, UnixMetadata>,
        req: &Request,
    ) -> crate::Result<FileAttr> {
        let entry_id = self.repo.entry_id(&path)?;
        let entry_inode = self.inodes.insert(path.clone(), entry_id);
        match self.entry_attr(&entry, entry_inode, req) {
            Ok(attr) => Ok(attr),
            Err(error) => {
                self.inodes.remove(entry_id, &path);
                Err(error)
            }
        }
    }

    /// Execute an atomic transaction.
    ///
    /// If `block` returns `Ok`, this function commits changes. If `block` returns `Err`, this
    /// function atomically rolls back all changes make in `block`.
    fn transaction<T>(
        &mut self,
        block: impl FnOnce(&mut Self) -> crate::Result<T>,
    ) -> crate::Result<T> {
        // We need to commit changes to all open objects because restoring to a savepoint will
        // invalidate them, causing all changes to be lost.
        self.objects.commit_all()?;

        let savepoint = self.repo.savepoint()?;
        let restore = self.repo.start_restore(&savepoint)?;
        match block(self) {
            Ok(result) => match self.repo.commit() {
                Ok(()) => Ok(result),
                Err(error) => {
                    self.repo.finish_restore(restore);
                    Err(error)
                }
            },
            Err(error) => {
                self.repo.finish_restore(restore);
                Err(error)
            }
        }
    }
}

impl<'a> Filesystem for FuseAdapter<'a> {
    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let file_name = try_option!(name.to_str(), reply, libc::ENOENT);
        let entry_path = try_option!(self.inodes.path(parent), reply, libc::ENOENT).join(file_name);
        let entry_id = try_result!(self.repo.entry_id(&entry_path), reply);
        let entry_inode = try_option!(self.inodes.inode(entry_id), reply, libc::ENOENT);
        let entry = try_result!(self.repo.entry(&entry_path), reply);

        let attr = try_result!(self.entry_attr(&entry, entry_inode, req), reply);

        let generation = self.inodes.generation(entry_inode);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT);
        let entry = try_result!(self.repo.entry(&entry_path), reply);
        let attr = try_result!(self.entry_attr(&entry, ino, req), reply);

        reply.attr(&DEFAULT_TTL, &attr);
    }

    fn setattr(
        &mut self,
        req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        _fh: Option<u64>,
        _crtime: Option<Timespec>,
        chgtime: Option<Timespec>,
        _bkuptime: Option<Timespec>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let now = SystemTime::now();

        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT).to_owned();

        // Whether the repository needs to be cleaned before this method returns.
        let mut needs_cleaned = false;

        let mut entry = try_result!(self.repo.entry(&entry_path), reply);

        let default_metadata = entry.default_metadata(req);
        entry.metadata.get_or_insert(default_metadata);

        let file_type = entry.kind;
        let mut metadata = entry.metadata.unwrap();

        if let Some(mode) = mode {
            metadata.change_permissions(mode);
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

        if let Some(ctime) = chgtime {
            metadata.changed = to_system_time(ctime);
        } else {
            metadata.changed = now;
        }

        let attr = try_result!(
            self.transaction(|fs| {
                // If `size` is no                        // Truncating the file should update its `mtime`, `atime`, and `ctime`.t `None`, that means we must truncate or extend the file.
                if let Some(new_size) = size {
                    let object = fs
                        .objects
                        .open_commit(ino, fs.repo.open(&entry_path).unwrap())?;

                    // If this method truncates the file to make it smaller, we need to clean the
                    // repository to free the unused space.
                    needs_cleaned = new_size < object.size().unwrap();

                    if new_size != object.size().unwrap() {
                        object.set_len(new_size)?;

                        // Truncating the file should update its `mtime`, `atime`, and `ctime`.
                        metadata.modified = now;
                        metadata.accessed = now;
                        metadata.changed = now;
                    }
                }

                fs.repo.set_metadata(&entry_path, Some(metadata.clone()))?;

                let entry = Entry {
                    kind: file_type,
                    metadata: Some(metadata),
                };
                fs.entry_attr(&entry, ino, req)
            }),
            reply
        );

        if needs_cleaned {
            // Attempt to clean the repository to free unused space. We ignore any errors because this
            // method must return successfully once the transaction is complete.
            self.repo.clean().ok();
        }

        reply.attr(&DEFAULT_TTL, &attr);
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT);
        let entry = try_result!(self.repo.entry(&entry_path), reply);
        match &entry.kind {
            EntryType::Special(UnixSpecial::Symlink { target }) => {
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
        let flags = SFlag::from_bits_truncate(mode);
        let file_name = try_option!(name.to_str(), reply, libc::EINVAL);
        let parent_path = try_option!(self.inodes.path(parent), reply, libc::ENOENT).to_owned();
        let entry_path = parent_path.join(file_name);

        let file_type = if flags.contains(SFlag::S_IFREG) {
            EntryType::File
        } else if flags.contains(SFlag::S_IFCHR) {
            let major = stat::major(rdev as u64);
            let minor = stat::minor(rdev as u64);
            EntryType::Special(UnixSpecial::CharDevice { major, minor })
        } else if flags.contains(SFlag::S_IFBLK) {
            let major = stat::major(rdev as u64);
            let minor = stat::minor(rdev as u64);
            EntryType::Special(UnixSpecial::BlockDevice { major, minor })
        } else if flags.contains(SFlag::S_IFIFO) {
            EntryType::Special(UnixSpecial::NamedPipe)
        } else if flags.contains(SFlag::S_IFSOCK) {
            // Sockets aren't supported by `FileRepo`. `mknod(2)` specifies that `EPERM`
            // should be returned if the file system doesn't support the type of node being
            // requested.
            reply.error(libc::EPERM);
            return;
        } else {
            // Other file types aren't supported by `mknod`.
            reply.error(libc::EINVAL);
            return;
        };

        let parent_entry = try_result!(self.repo.entry(&parent_path), reply);
        let entry = Entry {
            kind: file_type,
            metadata: None,
        }
        .with_metadata(req)
        .with_permissions(&parent_entry, Some(mode));

        let attr = try_result!(
            self.transaction(|fs| {
                fs.repo.create(&entry_path, &entry)?;
                fs.repo.touch_modified(&parent_path, req)?;
                fs.create_attr(entry_path, &entry, req)
            }),
            reply
        );

        let generation = self.inodes.generation(attr.ino);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn mkdir(&mut self, req: &Request, parent: u64, name: &OsStr, mode: u32, reply: ReplyEntry) {
        let file_name = try_option!(name.to_str(), reply, libc::EINVAL);
        let parent_path = try_option!(self.inodes.path(parent), reply, libc::ENOENT).to_owned();
        let entry_path = parent_path.join(file_name);

        let parent_entry = try_result!(self.repo.entry(&parent_path), reply);
        let entry = Entry::directory()
            .with_metadata(req)
            .with_permissions(&parent_entry, Some(mode));

        let attr = try_result!(
            self.transaction(|fs| {
                fs.repo.create(&entry_path, &entry)?;
                fs.repo.touch_modified(&parent_path, req)?;
                fs.create_attr(entry_path, &entry, req)
            }),
            reply
        );

        let generation = self.inodes.generation(attr.ino);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let file_name = try_option!(name.to_str(), reply, libc::ENOENT);
        let parent_path = try_option!(self.inodes.path(parent), reply, libc::ENOENT).to_owned();
        let entry_path = parent_path.join(file_name);
        let entry_id = try_result!(self.repo.entry_id(&entry_path), reply);
        let entry_inode = try_option!(self.inodes.inode(entry_id), reply, libc::ENOENT);

        if self.repo.is_directory(&entry_path) {
            reply.error(libc::EISDIR);
            return;
        }

        try_result!(
            self.transaction(|fs| {
                fs.repo.touch_changed(&entry_path, req)?;
                fs.repo.remove(&entry_path)?;
                fs.repo.touch_modified(&parent_path, req)
            }),
            reply
        );

        self.objects.close(entry_inode);
        self.inodes.remove(entry_id, &entry_path);

        // Attempt to clean the repository to free unused space. We ignore any errors because this
        // method must return successfully once the transaction is complete.
        self.repo.clean().ok();

        reply.ok();
    }

    fn rmdir(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let file_name = try_option!(name.to_str(), reply, libc::ENOENT);
        let parent_path = try_option!(self.inodes.path(parent), reply, libc::ENOENT).to_owned();
        let entry_path = parent_path.join(file_name);
        let entry_id = try_result!(self.repo.entry_id(&entry_path), reply);

        if !self.repo.is_directory(&entry_path) {
            reply.error(libc::ENOTDIR);
            return;
        }

        try_result!(
            self.transaction(|fs| {
                // `FileRepo::remove` method checks that the directory entry is empty.
                fs.repo.remove(&entry_path)?;
                fs.repo.touch_modified(&parent_path, req)
            }),
            reply
        );

        self.inodes.remove(entry_id, &entry_path);

        // Attempt to clean the repository to free unused space. We ignore any errors because this
        // method must return successfully once the transaction is complete.
        self.repo.clean().ok();

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
        let file_name = try_option!(name.to_str(), reply, libc::EINVAL);
        let parent_path = try_option!(self.inodes.path(parent), reply, libc::ENOENT).to_owned();
        let entry_path = parent_path.join(file_name);

        let parent_entry = try_result!(self.repo.entry(&parent_path), reply);
        let entry = Entry::special(UnixSpecial::Symlink {
            target: link.to_owned(),
        })
        .with_metadata(req)
        .with_permissions(&parent_entry, None);

        let attr = try_result!(
            self.transaction(|fs| {
                fs.repo.create(&entry_path, &entry)?;
                fs.repo.touch_modified(&parent_path, req)?;
                fs.create_attr(entry_path, &entry, req)
            }),
            reply
        );

        let generation = self.inodes.generation(attr.ino);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn rename(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEmpty,
    ) {
        let source_name = try_option!(name.to_str(), reply, libc::ENOENT);
        let source_parent_path =
            try_option!(self.inodes.path(parent), reply, libc::ENOENT).to_owned();
        let source_path = source_parent_path.join(source_name);
        let source_id = try_result!(self.repo.entry_id(&source_path), reply);
        let source_inode = try_option!(self.inodes.inode(source_id), reply, libc::ENOENT);

        let dest_name = try_option!(newname.to_str(), reply, libc::EINVAL);
        let dest_parent_path =
            try_option!(self.inodes.path(newparent), reply, libc::ENOENT).to_owned();
        let dest_path = dest_parent_path.join(dest_name);

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

        // Commit and close any open file objects associated with the entries in the source tree.
        try_result!(self.objects.commit(source_inode), reply);
        self.objects.close(source_inode);
        let walk_result = {
            // We need to borrow outside the closure because closures can't capture individual
            // fields.
            let inodes = &mut self.inodes;
            let objects = &mut self.objects;
            self.repo.walk(&source_path, |entry| {
                let descendant_inode = inodes.inode(entry.entry_id()).unwrap();
                if let Err(error) = objects.commit(descendant_inode) {
                    return WalkPredicate::Stop(error);
                }
                objects.close(descendant_inode);

                WalkPredicate::Continue
            })
        };
        if let Ok(Some(error)) = walk_result {
            try_result!(Err(error), reply);
        }

        let existing_dest_id = try_result!(
            self.transaction(|fs| {
                // Return the entry ID of the existing destination if it exists so we can remove it
                // from the inode table.
                let existing_dest_id = fs.repo.entry_id(&dest_path).ok();

                // Remove the destination path unless it is a non-empty directory.
                if let Err(error @ crate::Error::NotEmpty) = fs.repo.remove(&dest_path) {
                    return Err(error);
                }

                fs.repo.rename(&source_path, &dest_path)?;

                fs.repo.touch_modified(&source_parent_path, req)?;
                fs.repo.touch_modified(&dest_parent_path, req)?;

                Ok(existing_dest_id)
            }),
            reply
        );

        // If the destination entry already existed, we need to remove it from the inode table.
        if let Some(entry_id) = existing_dest_id {
            assert!(self.inodes.remove(entry_id, &dest_path));
        }

        // Update the path of the entry in the node table.
        assert!(self
            .inodes
            .remap(source_inode, &source_path, dest_path.clone()));

        // Update the paths of any descendants of the entry in the node table.
        {
            // We need to borrow outside the closure because closures can't capture individual
            // fields.
            let inodes = &mut self.inodes;
            self.repo
                .walk::<(), _, _>(&dest_path, |entry| {
                    let descendant_inode = inodes.inode(entry.entry_id()).unwrap();
                    let relative_path = entry.path().strip_prefix(&dest_path).unwrap();
                    let original_path = source_path.join(relative_path);
                    assert!(inodes.remap(descendant_inode, &original_path, entry.into_path()));
                    WalkPredicate::Continue
                })
                .ok();
        }

        reply.ok();
    }

    fn link(
        &mut self,
        req: &Request,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let dest_name = try_option!(newname.to_str(), reply, libc::EINVAL);
        let source_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT).to_owned();
        let dest_parent_path =
            try_option!(self.inodes.path(newparent), reply, libc::ENOENT).to_owned();
        let dest_path = dest_parent_path.join(dest_name);
        let source_id = self.repo.entry_id(&source_path).unwrap();

        let attr = try_result!(
            self.transaction(|fs| {
                fs.repo.link(&source_path, &dest_path)?;
                fs.repo.touch_changed(&source_path, req)?;
                fs.repo.touch_modified(&dest_parent_path, req)?;
                let entry = fs.repo.entry(&source_path)?;
                fs.entry_attr(&entry, ino, req)
            }),
            reply
        );

        self.inodes.insert(dest_path, source_id);
        let generation = self.inodes.generation(ino);

        reply.entry(&DEFAULT_TTL, &attr, generation);
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        let flags = OFlag::from_bits_truncate(flags as i32);

        if flags.intersects(*UNSUPPORTED_OPEN_FLAGS) {
            reply.error(libc::ENOTSUP);
            return;
        }

        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT);

        if !self.repo.exists(&entry_path) {
            reply.error(libc::ENOENT);
            return;
        }

        if !self.repo.is_file(&entry_path) {
            reply.error(libc::ENOTSUP);
            return;
        }

        let state = HandleState::File(FileHandle { flags, position: 0 });
        let fh = self.handles.open(state);

        reply.opened(fh, 0);
    }

    fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, size: u32, reply: ReplyData) {
        // Technically, on Unix systems, a file should still be accessible via its file descriptor
        // once it's been unlinked. Because this isn't how repositories work, we will return `EBADF`
        // if the user tries to read from a file which has been unlinked since it was opened.
        let entry_path = match self.inodes.path(ino) {
            Some(path) => path.to_owned(),
            None => {
                self.handles.close(fh);
                reply.error(libc::EBADF);
                return;
            }
        };

        let state = match self.handles.state_mut(fh) {
            None => {
                reply.error(libc::EBADF);
                return;
            }
            Some(HandleState::Directory(_)) => {
                reply.error(libc::EISDIR);
                return;
            }
            Some(HandleState::File(state)) => state,
        };

        let mut buffer = vec![0u8; size as usize];
        let mut total_bytes_read = 0;

        {
            let object = try_result!(
                self.objects
                    .open_commit(ino, self.repo.open(&entry_path).unwrap()),
                reply
            );
            try_result!(object.seek(SeekFrom::Start(offset as u64)), reply);

            // `Filesystem::read` should read the exact number of bytes requested except on EOF or error.
            let mut bytes_read;
            loop {
                bytes_read = try_result!(
                    object.read(&mut buffer[total_bytes_read..size as usize]),
                    reply
                );
                total_bytes_read += bytes_read;

                if bytes_read == 0 {
                    // Either the object has reached EOF or we've already read `size` bytes from it.
                    break;
                }
            }
        }

        state.position = offset as u64 + total_bytes_read as u64;

        // Update the file's `st_atime` unless the `O_NOATIME` flag was passed.
        if !state.flags.contains(OFlag::O_NOATIME) {
            try_result!(self.repo.touch_accessed(&entry_path, req), reply);
        }

        reply.data(&buffer[..total_bytes_read]);
    }

    fn write(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: u32,
        reply: ReplyWrite,
    ) {
        // Technically, on Unix systems, a file should still be accessible via its file descriptor
        // once it's been unlinked. Because this isn't how repositories work, we will return `EBADF`
        // if the user tries to write to a file which has been unlinked since it was opened.
        let entry_path = match self.inodes.path(ino) {
            Some(path) => path.to_owned(),
            None => {
                self.handles.close(fh);
                reply.error(libc::EBADF);
                return;
            }
        };

        let flags;

        {
            let state = match self.handles.state_mut(fh) {
                None => {
                    reply.error(libc::EBADR);
                    return;
                }
                Some(HandleState::Directory(_)) => {
                    reply.error(libc::EISDIR);
                    return;
                }
                Some(HandleState::File(state)) => state,
            };

            flags = state.flags;

            let object = if offset as u64 == state.position {
                // If the offset is the same as the previous offset, we don't need to seek and
                // therefore don't need to commit changes to the object.
                self.objects.open(ino, self.repo.open(&entry_path).unwrap())
            } else {
                // If the offset is not the same as the previous offset, we need to seek, which
                // requires committing changes first.
                let object = try_result!(
                    self.objects
                        .open_commit(ino, self.repo.open(&entry_path).unwrap()),
                    reply
                );

                let object_size = object.size().unwrap();

                // If the offset is past the end of the file, we need to extend it. It's not
                // possible to seek past the end of an object.
                if offset as u64 > object_size {
                    try_result!(object.set_len(offset as u64), reply);
                }

                try_result!(object.seek(SeekFrom::Start(offset as u64)), reply);

                object
            };

            try_result!(object.write_all(data), reply);

            state.position = offset as u64 + data.len() as u64;
        }

        // After this point, we need to be more careful about error handling. Because bytes have
        // been written to the object, if an error occurs, we need to drop the `Object` to discard
        // any uncommitted changes before returning so that bytes will only have been written to the
        // object if this method returns successfully.

        // Update the `st_atime` and `st_mtime` for the entry.
        if let Err(error) = self.repo.touch_modified(&entry_path, req) {
            self.objects.close(ino);
            reply.error(error.to_errno());
            return;
        }

        // If the `O_SYNC` or `O_DSYNC` flags were passed, we need to commit changes to the object
        // *and* commit changes to the repository after each write.
        if flags.intersects(OFlag::O_SYNC | OFlag::O_DSYNC) {
            if let Err(error) = self.objects.commit(ino) {
                self.objects.close(ino);
                reply.error(error.to_errno());
                return;
            }

            if let Err(error) = self.repo.commit() {
                self.objects.close(ino);
                reply.error(error.to_errno());
                return;
            }
        }

        reply.written(data.len() as u32);
    }

    fn flush(&mut self, _req: &Request, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        try_result!(self.objects.commit(ino), reply);
        reply.ok()
    }

    fn release(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.handles.close(fh);
        self.objects.close(ino);
        reply.ok()
    }

    fn fsync(&mut self, _req: &Request, ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        try_result!(self.objects.commit(ino), reply);
        try_result!(self.repo.commit(), reply);
        reply.ok();
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT);

        if !self.repo.is_directory(entry_path) {
            reply.error(libc::ENOTDIR);
            return;
        }

        let mut entries = Vec::new();
        let walk_result = self.repo.walk(entry_path, |entry| {
            let file_name = entry.path().file_name().unwrap().to_string();
            let inode = self.inodes.inode(entry.entry_id()).unwrap();
            let file_type = match self.repo.entry(entry.path()) {
                Ok(entry) => entry.kind.to_file_type(),
                Err(error) => return WalkPredicate::Stop(error),
            };
            entries.push(DirectoryEntry {
                file_name,
                file_type,
                inode,
            });
            // We only want immediate children.
            WalkPredicate::SkipDescendants
        });

        match walk_result {
            Err(error) => try_result!(Err(error), reply),
            Ok(Some(error)) => try_result!(Err(error), reply),
            Ok(None) => {}
        }

        let state = HandleState::Directory(DirectoryHandle { entries });
        let fh = self.handles.open(state);

        reply.opened(fh, 0);
    }

    fn readdir(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let directory_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT).to_owned();

        try_result!(
            self.transaction(|fs| fs.repo.touch_accessed(&directory_path, req)),
            reply
        );

        let entries = match self.handles.state(fh) {
            None => {
                reply.error(libc::EBADF);
                return;
            }
            Some(HandleState::File(_)) => {
                reply.error(libc::ENOTDIR);
                return;
            }
            Some(HandleState::Directory(DirectoryHandle { entries })) => entries,
        };

        for (i, dir_entry) in entries[offset as usize..].iter().enumerate() {
            if reply.add(
                dir_entry.inode,
                (i + 1) as i64,
                dir_entry.file_type,
                &dir_entry.file_name,
            ) {
                break;
            }
        }

        reply.ok();
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: u32, reply: ReplyEmpty) {
        self.handles.close(fh);
        reply.ok()
    }

    fn fsyncdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        try_result!(self.repo.commit(), reply);
        reply.ok();
    }

    fn setxattr(
        &mut self,
        req: &Request,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let attr_name = try_option!(name.to_str(), reply, libc::EINVAL).to_owned();

        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT).to_owned();
        let mut metadata =
            try_result!(self.repo.entry(&entry_path), reply).metadata_or_default(req);

        if flags == 0 {
            metadata
                .attributes
                .insert(attr_name.clone(), value.to_vec());
        } else if flags == libc::XATTR_CREATE as u32 {
            match metadata.attributes.entry(attr_name.clone()) {
                HashMapEntry::Occupied(_) => {
                    reply.error(libc::EEXIST);
                    return;
                }
                HashMapEntry::Vacant(entry) => {
                    entry.insert(value.to_vec());
                }
            }
        } else if flags == libc::XATTR_REPLACE as u32 {
            match metadata.attributes.entry(attr_name.clone()) {
                HashMapEntry::Occupied(mut entry) => {
                    entry.insert(value.to_vec());
                }
                HashMapEntry::Vacant(_) => {
                    reply.error(libc::ENODATA);
                    return;
                }
            }
        } else {
            reply.error(libc::EINVAL);
            return;
        }

        // Synchronize the ACL entries stored in the xattrs with the entry metadata.
        match attr_name.as_str() {
            ACCESS_ACL_XATTR => {
                let mut permissions = Permissions::from(metadata.clone());
                try_result!(permissions.update_attr(&attr_name, value), reply);
                metadata.mode = FileMode::from_bits_truncate(permissions.mode);
                metadata.acl.access = permissions.acl.access;
            }
            DEFAULT_ACL_XATTR => {
                let mut permissions = Permissions::from(metadata.clone());
                try_result!(permissions.update_attr(&attr_name, value), reply);
                metadata.mode = FileMode::from_bits_truncate(permissions.mode);
                metadata.acl.default = permissions.acl.default;
            }
            _ => {}
        }

        // Update the ctime whenever xattrs are modified.
        metadata.changed = SystemTime::now();

        try_result!(
            self.transaction(|fs| fs.repo.set_metadata(entry_path, Some(metadata))),
            reply
        );

        reply.ok();
    }

    fn getxattr(&mut self, req: &Request, ino: u64, name: &OsStr, size: u32, reply: ReplyXattr) {
        let attr_name = try_option!(name.to_str(), reply, libc::ENODATA).to_owned();

        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT);
        let mut metadata =
            try_result!(self.repo.entry(&entry_path), reply).metadata_or_default(req);

        // `UnixMetadata.acl` is the single source of truth for ACL entries. We should intercept
        // attempts to read the ACL xattr and generate its value from the ACL entries in the
        // entry metadata instead of reading from the xattrs in the entry metadata.
        let attr_value = match attr_name.as_str() {
            ACCESS_ACL_XATTR if metadata.acl.access.is_empty() => {
                // If there are no ACL entries, the attr should not be set.
                reply.error(libc::ENODATA);
                return;
            }
            DEFAULT_ACL_XATTR if metadata.acl.default.is_empty() => {
                // If there are no ACL entries, the attr should not be set.
                reply.error(libc::ENODATA);
                return;
            }
            ACCESS_ACL_XATTR | DEFAULT_ACL_XATTR => {
                try_result!(Permissions::from(metadata).to_attr(&attr_name), reply)
            }
            _ => {
                try_option!(metadata.attributes.remove(&attr_name), reply, libc::ENODATA)
            }
        };

        if size == 0 {
            reply.size(attr_value.len() as u32);
            return;
        }

        if attr_value.len() > size as usize {
            reply.error(libc::ERANGE);
            return;
        }

        reply.data(attr_value.as_slice());
    }

    fn listxattr(&mut self, req: &Request, ino: u64, size: u32, reply: ReplyXattr) {
        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT);
        let metadata = try_result!(self.repo.entry(&entry_path), reply).metadata_or_default(req);

        // Construct a byte string of null-terminated attribute names.
        let mut attr_names = Vec::new();
        for attr_name in metadata.attributes.keys() {
            attr_names.extend_from_slice(attr_name.as_bytes());
            attr_names.push(0u8);
        }

        if size == 0 {
            reply.size(attr_names.len() as u32);
            return;
        }

        if attr_names.len() > size as usize {
            reply.error(libc::ERANGE);
            return;
        }

        reply.data(attr_names.as_slice());
    }

    fn removexattr(&mut self, req: &Request, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let attr_name = try_option!(name.to_str(), reply, libc::ENODATA).to_owned();

        let entry_path = try_option!(self.inodes.path(ino), reply, libc::ENOENT).to_owned();
        let mut metadata =
            try_result!(self.repo.entry(&entry_path), reply).metadata_or_default(req);

        metadata.attributes.remove(&attr_name);

        // Synchronize the ACL entries stored in the xattrs with the entry metadata.
        match attr_name.as_str() {
            ACCESS_ACL_XATTR => {
                metadata.acl.access.clear();
            }
            DEFAULT_ACL_XATTR => {
                metadata.acl.default.clear();
            }
            _ => {}
        }

        // Update the ctime whenever xattrs are modified.
        metadata.changed = SystemTime::now();

        try_result!(
            self.transaction(|fs| fs.repo.set_metadata(entry_path, Some(metadata))),
            reply
        );

        reply.ok();
    }
}
