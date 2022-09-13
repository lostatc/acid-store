use std::collections::HashMap;

use fuse::FileType as FuseFileType;
use nix::fcntl::OFlag;

use super::id_table::IdTable;

/// A directory entry for an open file handle.
#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub file_name: String,
    pub file_type: FuseFileType,
    pub inode: u64,
}

/// The state associated with a file handle.
#[derive(Debug, Clone)]
pub struct FileHandle {
    /// The flags the handle was opened with.
    pub flags: OFlag,

    /// The current seek position of the file.
    pub position: u64,
}

/// The state associated with a directory handle.
#[derive(Debug, Clone)]
pub struct DirectoryHandle {
    /// The list of directory entries for this directory handle.
    pub entries: Vec<DirectoryEntry>,
}

/// The state associated with a file or directory handle.
#[derive(Debug, Clone)]
pub enum HandleState {
    File(FileHandle),
    Directory(DirectoryHandle),
}

/// A table for allocating file handles in a virtual file system.
#[derive(Debug, Clone, Default)]
pub struct HandleTable {
    /// The table which uniquely allocates integers to act as file handles.
    id_table: IdTable,

    /// A map of file handles to the flags they were opened with.
    state: HashMap<u64, HandleState>,
}

impl HandleTable {
    /// Return a new empty `HandleTable`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a new file handle with the given `state`.
    pub fn open(&mut self, state: HandleState) -> u64 {
        let fh = self.id_table.next();
        self.state.insert(fh, state);
        fh
    }

    /// Remove the given `fh` from the table.
    pub fn close(&mut self, fh: u64) {
        self.id_table.recycle(fh);
        self.state.remove(&fh);
    }

    /// Get the state associated with the given `fh`.
    pub fn state(&self, fh: u64) -> Option<&HandleState> {
        self.state.get(&fh)
    }

    /// Get the state associated with the given `fh`.
    pub fn state_mut(&mut self, fh: u64) -> Option<&mut HandleState> {
        self.state.get_mut(&fh)
    }
}
