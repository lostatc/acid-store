/// A mount option accepted when mounting a FUSE file system.
///
/// See `man mount.fuse` for details.
#[cfg_attr(docsrs, doc(cfg(all(unix, feature = "fuse-mount"))))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MountOption {
    /// Set the name of the source in mtab.
    FsName(String),

    /// Set the filesystem subtype in mtab.
    Subtype(String),

    /// Allow all users to access files on this filesystem. By default access is restricted to the
    /// user who mounted it.
    AllowOther,

    /// Allow the root user to access this filesystem, in addition to the user who mounted it.
    AllowRoot,

    /// Automatically unmount when the mounting process exits.
    ///
    /// `AutoUnmount` requires `AllowOther` or `AllowRoot`. If `AutoUnmount` is set and neither of
    /// those is set, the FUSE configuration must permit `allow_other`, otherwise mounting will
    /// fail.
    AutoUnmount,

    /// Enable permission checking in the kernel.
    DefaultPermissions,

    /// Enable special character and block devices.
    Dev,

    /// Disable special character and block devices.
    NoDev,

    /// Honor set-user-id and set-groupd-id bits on files.
    Suid,

    /// Don't honor set-user-id and set-groupd-id bits on files.
    NoSuid,

    /// Read-only filesystem.
    Ro,

    /// Read-write filesystem.
    Rw,

    /// Allow execution of binaries.
    Exec,

    /// Don't allow execution of binaries.
    NoExec,

    /// Support inode access time.
    Atime,

    /// Don't update inode access time.
    NoAtime,

    /// All modifications to directories will be done synchronously.
    DirSync,

    /// All I/O will be done synchronously.
    Sync,

    /// All I/O will be done asynchronously.
    Async,

    /// Allows passing an option which is not otherwise supported in these enums.
    Custom(String),
}

impl MountOption {
    pub(crate) fn into_fuser(self) -> fuser::MountOption {
        use fuser::MountOption::*;

        match self {
            Self::FsName(name) => FSName(name),
            Self::Subtype(name) => Subtype(name),
            Self::AllowOther => AllowOther,
            Self::AllowRoot => AllowRoot,
            Self::AutoUnmount => AutoUnmount,
            Self::DefaultPermissions => DefaultPermissions,
            Self::Dev => Dev,
            Self::NoDev => NoDev,
            Self::Suid => Suid,
            Self::NoSuid => NoSuid,
            Self::Ro => RO,
            Self::Rw => RW,
            Self::Exec => Exec,
            Self::NoExec => NoExec,
            Self::Atime => Atime,
            Self::NoAtime => NoAtime,
            Self::DirSync => DirSync,
            Self::Sync => Sync,
            Self::Async => Async,
            Self::Custom(value) => CUSTOM(value),
        }
    }
}
