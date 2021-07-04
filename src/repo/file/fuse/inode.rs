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

use bimap::BiMap;
use fuse::FUSE_ROOT_ID;
use relative_path::{RelativePath, RelativePathBuf};

use crate::repo::common::IdTable;

/// A table for allocating inodes in a virtual file system.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct InodeTable {
    /// The table which uniquely allocates integers to act as inodes.
    id_table: IdTable,

    /// A map of inodes to paths of entries in the repository.
    paths: BiMap<u64, RelativePathBuf>,

    /// A map of inode numbers to their generations.
    ///
    /// Generations are a concept in libfuse in which an additional integer ID is associated with
    /// each inode to ensure they're unique even when the inode values are reused.
    ///
    /// If an inode is not in this map, its generation is `0`.
    generations: HashMap<u64, u64>,
}

impl InodeTable {
    /// Return a new empty `InodeTable`.
    pub fn new(root: &RelativePath) -> Self {
        let mut table = Self::default();
        // Add the root entry to the table.
        table.paths.insert(FUSE_ROOT_ID, root.to_owned());
        table
    }

    /// Return whether the given `inode` is in the table.
    pub fn contains_inode(&self, inode: u64) -> bool {
        self.paths.contains_left(&inode)
    }

    /// Return whether the given `path` is in the table.
    pub fn contains_path(&self, path: &RelativePath) -> bool {
        self.paths.contains_right(path)
    }

    /// Insert the given `path` into the table and return its inode.
    pub fn insert(&mut self, path: RelativePathBuf) -> u64 {
        let inode = self.id_table.next();
        self.paths.insert(inode, path);
        inode
    }

    /// Remove the given `inode` from the table.
    ///
    /// This returns the path associated with the `inode` or `None` if the given `inode` is not in
    /// the table.
    pub fn remove(&mut self, inode: u64) -> Option<RelativePathBuf> {
        if !self.id_table.recycle(inode) {
            return None;
        }
        let generation = self.generations.entry(inode).or_default();
        *generation += 1;
        Some(self.paths.remove_by_left(&inode).unwrap().1)
    }

    /// Get the path associated with `inode` or `None` if it is not in the table.
    pub fn path(&self, inode: u64) -> Option<&RelativePath> {
        self.paths
            .get_by_left(&inode)
            .map(|path| path.as_relative_path())
    }

    /// Get the inode associated with `path` or `None` if it is not in the table.
    pub fn inode(&self, path: &RelativePath) -> Option<u64> {
        self.paths.get_by_right(path).copied()
    }

    /// Return the generation number associated with the given `inode`.
    pub fn generation(&self, inode: u64) -> u64 {
        self.generations.get(&inode).copied().unwrap_or(0)
    }
}
