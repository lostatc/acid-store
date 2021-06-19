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

use std::collections::{HashMap, HashSet};

use super::metadata::UnixMetadata;
use super::repository::{FileRepo, EMPTY_PARENT};
use super::special::UnixSpecialType;
use super::RelativePathBuf;

/// A table for allocating `UniqueId` values.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct InodeTable {
    /// The highest used ID value (the high water mark).
    highest: u64,

    /// A set of unused ID values below the high water mark.
    unused: HashSet<u64>,
}

impl InodeTable {
    /// Return a new empty `IdTable`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the next unused ID from the table.
    pub fn next(&mut self) -> u64 {
        match self.unused.iter().next().copied() {
            Some(inode) => {
                self.unused.remove(&inode);
                inode
            }
            None => {
                self.highest += 1;
                self.highest
            }
        }
    }

    /// Return whether the given `inode` is in the table.
    pub fn contains(&self, inode: u64) -> bool {
        inode <= self.highest && !self.unused.contains(&inode)
    }

    /// Return the given `inode` back to the table.
    ///
    /// This returns `true` if the value was returned or `false` if it was unused.
    pub fn recycle(&mut self, inode: u64) -> bool {
        if !self.contains(inode) {
            return false;
        }
        self.unused.insert(inode);
        true
    }
}

pub struct FuseAdapter<'a> {
    /// The repository which contains the virtual file system.
    repo: &'a mut FileRepo<UnixSpecialType, UnixMetadata>,

    // Because the underlying FUSE library uses inodes instead of file paths, we need to store a
    // map of inodes to paths. This will consume significant additional memory if the repository
    // contains many paths, and it doesn't benefit from the repository's ability to reduce the
    // memory footprint of file paths by using a prefix tree.
    /// A map of inodes to paths of entries in the repository.
    inodes: HashMap<u64, RelativePathBuf>,

    /// A table for allocating inodes.
    inode_table: InodeTable,
}

impl<'a> FuseAdapter<'a> {
    pub fn new(repo: &'a mut FileRepo<UnixSpecialType, UnixMetadata>) -> Self {
        let mut inode_table = InodeTable::new();
        let mut inodes = HashMap::new();

        for (path, _) in repo.0.state().walk(&*EMPTY_PARENT).unwrap() {
            let inode = inode_table.next();
            inodes.insert(inode, path);
        }

        Self {
            repo,
            inodes,
            inode_table,
        }
    }
}
