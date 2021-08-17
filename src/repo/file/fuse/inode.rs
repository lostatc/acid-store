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

use std::collections::{HashMap, HashSet};

use bimap::BiMap;
use fuse::FUSE_ROOT_ID;
use relative_path::{RelativePath, RelativePathBuf};

use super::id_table::IdTable;
use crate::repo::file::EntryId;
use std::collections::hash_map::Entry;

/// A table for allocating inodes in a virtual file system.
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct InodeTable {
    /// The table which uniquely allocates integers to act as inodes.
    id_table: IdTable,

    /// A map of entry IDs to their inodes in the file system.
    entries: BiMap<EntryId, u64>,

    /// A map of inodes to the set of paths which refer to the entry.
    paths: HashMap<u64, HashSet<RelativePathBuf>>,

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
        let mut table = Self {
            id_table: IdTable::with_reserved(vec![FUSE_ROOT_ID]),
            entries: BiMap::new(),
            paths: HashMap::new(),
            generations: HashMap::new(),
        };
        // Add the root entry to the table.
        let mut root_paths = HashSet::new();
        root_paths.insert(root.to_owned());
        table.paths.insert(FUSE_ROOT_ID, root_paths);

        table
    }

    /// Return whether the entry with the given `inode` is in the table.
    pub fn contains_inode(&self, inode: u64) -> bool {
        self.entries.contains_right(&inode)
    }

    /// Return whether the entry with the given `id` is in the table.
    pub fn contains_entry(&self, id: EntryId) -> bool {
        self.entries.contains_left(&id)
    }

    /// Insert the given `path` and entry `id` into the table and return the entry's inode.
    pub fn insert(&mut self, path: RelativePathBuf, id: EntryId) -> u64 {
        if !self.entries.contains_left(&id) {
            let inode = self.id_table.next();
            self.entries.insert(id, inode);
        }
        let inode = self.entries.get_by_left(&id).copied().unwrap();
        self.paths
            .entry(inode)
            .or_insert_with(HashSet::new)
            .insert(path);
        inode
    }

    /// Remove the entry with the given `id` and `path` from the table.
    ///
    /// This returns `true` if the entry was removed or `false` if it did not exist in the table.
    pub fn remove(&mut self, id: EntryId, path: &RelativePath) -> bool {
        let inode = match self.entries.get_by_left(&id) {
            Some(inode) => *inode,
            None => return false,
        };

        match self.paths.entry(inode) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().remove(path);
                if entry.get().is_empty() {
                    entry.remove();
                    self.entries.remove_by_right(&inode);
                    self.id_table.recycle(inode);
                    let generation = self.generations.entry(inode).or_default();
                    *generation += 1;
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }

        true
    }

    /// Change one of the paths associated with the given `inode`.
    ///
    /// This returns `true` if the path was changed or `false` if `inode` is not in the table or
    /// `old_path` is not associated with `inode`.
    pub fn remap(
        &mut self,
        inode: u64,
        old_path: &RelativePath,
        new_path: RelativePathBuf,
    ) -> bool {
        match self.paths.get_mut(&inode) {
            None => false,
            Some(paths) => {
                if !paths.remove(old_path) {
                    return false;
                }
                paths.insert(new_path);
                true
            }
        }
    }

    /// Get a path associated with `inode` or `None` if it is not in the table.
    pub fn path(&self, inode: u64) -> Option<&RelativePath> {
        self.paths
            .get(&inode)
            .map(|path_set| path_set.iter().next().unwrap().as_ref())
    }

    /// Get the set of paths associated with `inode` or `None` if it is not in the table.
    pub fn paths(&self, inode: u64) -> Option<&HashSet<RelativePathBuf>> {
        self.paths.get(&inode)
    }

    /// Get the inode associated with the given entry `id` or `None` if it is not in the table.
    pub fn inode(&self, id: EntryId) -> Option<u64> {
        self.entries.get_by_left(&id).copied()
    }

    /// Return the generation number associated with the given `inode`.
    pub fn generation(&self, inode: u64) -> u64 {
        self.generations.get(&inode).copied().unwrap_or(0)
    }
}
