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

use std::collections::HashMap;

use nix::fcntl::OFlag;

use crate::repo::common::IdTable;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleType {
    File,
    Directory,
}

/// Information about a file handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandleInfo {
    /// The flags used to open the file.
    pub flags: OFlag,

    /// Whether the handle refers to a file or directory.
    pub handle_type: HandleType,
}

/// A table for allocating file handles in a virtual file system.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct HandleTable {
    /// The table which uniquely allocates integers to act as file handles.
    id_table: IdTable,

    /// A map of file handles to the flags they were opened with.
    info: HashMap<u64, HandleInfo>,
}

impl HandleTable {
    /// Return a new empty `HandleTable`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a new file handle for the file opened with the given `flags`.
    pub fn open(&mut self, flags: OFlag, handle_type: HandleType) -> u64 {
        let fh = self.id_table.next();
        let info = HandleInfo { flags, handle_type };
        self.info.insert(fh, info);
        fh
    }

    /// Remove the given `fh` from the table.
    pub fn close(&mut self, fh: u64) {
        self.id_table.recycle(fh);
        self.info.remove(&fh);
    }

    /// Get information about the given `fh`.
    pub fn info(&self, fh: u64) -> Option<HandleInfo> {
        self.info.get(&fh).copied()
    }
}
