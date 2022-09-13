use std::collections::{hash_map::Entry as HashMapEntry, HashMap};

use crate::repo::Object;
use std::collections::hash_map::Entry;

/// A table of open `Object` values representing open files.
///
/// Objects in this table may be invalidated, in which case they are dropped lazily.
#[derive(Debug)]
pub struct ObjectTable(HashMap<u64, Object>);

impl ObjectTable {
    /// Return a new empty `ObjectTable`.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Return an `Object` for the file at the given `inode`.
    ///
    /// If the object is not currently open or has been invalidated, then `default` will be inserted
    /// into the object table.
    ///
    /// The returned object may have a transaction in progress.
    pub fn open(&mut self, inode: u64, default: Object) -> &mut Object {
        match self.0.entry(inode) {
            Entry::Occupied(mut object_entry) => {
                if !object_entry.get().is_valid() {
                    object_entry.insert(default);
                }
                object_entry.into_mut()
            }
            Entry::Vacant(object_entry) => object_entry.insert(default),
        }
    }

    /// Commit changes to the `Object` for the file at the given `inode` if it is open.
    ///
    /// If the object is not open or has been invalidated, this returns `Ok`.
    pub fn commit(&mut self, inode: u64) -> crate::Result<()> {
        if let HashMapEntry::Occupied(mut object_entry) = self.0.entry(inode) {
            if object_entry.get().is_valid() {
                object_entry.get_mut().commit()?;
            } else {
                object_entry.remove();
            }
        }
        Ok(())
    }

    /// Commit changes to all objects in the table which have not been invalidated.
    pub fn commit_all(&mut self) -> crate::Result<()> {
        let inodes = self.0.keys().copied().collect::<Vec<_>>();
        for inode in inodes {
            self.commit(inode)?;
        }

        Ok(())
    }

    /// Return an `Object` for the file at the given `inode`.
    ///
    /// If the object is not currently open or has been invalidated, then `default` will be inserted
    /// into the object table.
    ///
    /// This commits changes if the object was already open to ensure there is not a transaction in
    /// progress when this method returns.
    pub fn open_commit(&mut self, inode: u64, default: Object) -> crate::Result<&mut Object> {
        match self.0.entry(inode) {
            Entry::Occupied(mut object_entry) => {
                if object_entry.get().is_valid() {
                    object_entry.get_mut().commit()?;
                } else {
                    object_entry.insert(default);
                }
                Ok(object_entry.into_mut())
            }
            Entry::Vacant(object_entry) => Ok(object_entry.insert(default)),
        }
    }

    /// Close the object for the file at the given `inode` if it is open.
    pub fn close(&mut self, inode: u64) -> bool {
        self.0.remove(&inode).is_some()
    }
}
