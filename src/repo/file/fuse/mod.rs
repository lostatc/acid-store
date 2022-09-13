#![cfg(all(any(unix, doc), feature = "fuse-mount"))]

pub use self::fs::FuseAdapter;

mod acl;
mod fs;
mod handle;
mod id_table;
mod inode;
mod metadata;
mod object;
