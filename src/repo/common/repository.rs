/*
 * Copyright 2019-2020 Wren Powell
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

use std::borrow::Borrow;
use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::mem;
use std::sync::Arc;

use hex_literal::hex;
use rmp_serde::{from_read, to_vec};
use secrecy::ExposeSecret;
use uuid::Uuid;

use crate::repo::{OpenRepo, Packing};
use crate::store::DataStore;

use super::chunk_store::{
    EncodeBlock, ReadBlock, ReadChunk, StoreReader, StoreState, StoreWriter, WriteBlock,
};
use super::encryption::{EncryptionKey, KeySalt};
use super::id_table::IdTable;
use super::key::Key;
use super::metadata::{Header, RepoInfo};
use super::object::{chunk_hash, Object, ObjectHandle, ReadOnlyObject};
use super::savepoint::Savepoint;
use super::state::RepoState;

/// The block ID of the block which stores the repository metadata.
pub(super) const METADATA_BLOCK_ID: Uuid =
    Uuid::from_bytes(hex!("8691d360 29c6 11ea 8bc1 2fc8cfe66f33"));

/// The block ID of the block which stores the repository format version.
pub(super) const VERSION_BLOCK_ID: Uuid =
    Uuid::from_bytes(hex!("cbf28b1c 3550 11ea 8cb0 87d7a14efe10"));

/// An object store which maps keys to seekable binary blobs.
///
/// See [`crate::repo::key`] for more information.
#[derive(Debug)]
pub struct KeyRepo<K: Key> {
    /// The state for this repository.
    pub(super) state: RepoState,

    /// The instance ID of this repository instance.
    pub(super) instance_id: Uuid,

    /// A map of object keys to their object handles for the current instance.
    pub(super) objects: HashMap<K, ObjectHandle>,

    /// A map of instance IDs to the object handles which store their object maps.
    ///
    /// Each object handle in this map contains a serialized map of object IDs to object handles for
    /// that instance. When switching repository instances, that map is read from the object,
    /// deserialized, and moved to `objects`.
    pub(super) instances: HashMap<Uuid, ObjectHandle>,

    /// A table of unique IDs of existing handles.
    ///
    /// We use this to determine whether a handle is contained in the repository without actually
    /// storing it.
    pub(super) handle_table: IdTable,

    /// The unique ID for the current transaction.
    ///
    /// This ID changes each time the repository is opened or committed. It is used to invalidate
    /// savepoints.
    pub(super) transaction_id: Arc<Uuid>,
}

impl<K: Key> OpenRepo for KeyRepo<K> {
    type Key = K;

    const VERSION_ID: Uuid = Uuid::from_bytes(hex!("989a6a76 9d8b 46b7 9c05 d1c5e0d9471a"));

    fn open_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(repo)
    }

    fn create_repo(repo: KeyRepo<Self::Key>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(repo)
    }

    fn into_repo(self) -> crate::Result<KeyRepo<Self::Key>> {
        Ok(self)
    }
}

impl<K: Key> KeyRepo<K> {
    /// Return whether there is an object with the given `key` in this repository.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.objects.contains_key(key)
    }

    /// Add a new object with the given `key` to the repository and return it.
    ///
    /// If another object with the same `key` already exists, it is replaced.
    pub fn insert(&mut self, key: K) -> Object {
        self.remove(&key);
        let handle = ObjectHandle {
            id: self.handle_table.next(),
            chunks: Vec::new(),
        };
        assert!(!self.objects.contains_key(&key));
        let handle = self.objects.entry(key).or_insert(handle);
        Object::new(&mut self.state, handle)
    }

    /// Remove the given object `handle` from the repository.
    fn remove_handle(&mut self, handle: &ObjectHandle) {
        for chunk in &handle.chunks {
            let chunk_info = self
                .state
                .chunks
                .get_mut(chunk)
                .expect("This chunk was not found in the repository.");
            chunk_info.references.remove(&handle.id);
            if chunk_info.references.is_empty() {
                self.state.chunks.remove(chunk);
            }
        }
        self.handle_table.recycle(handle.id);
    }

    /// Remove the object with the given `key` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`clean`] is called.
    ///
    /// [`clean`]: crate::repo::key::KeyRepo::clean
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = match self.objects.remove(key) {
            Some(handle) => handle,
            None => return false,
        };
        self.remove_handle(&handle);
        true
    }

    /// Return a `ReadOnlyObject` for reading the object with the given `key`.
    ///
    /// This returns `None` if there is no object with the given `key` in the repository.
    ///
    /// The returned object provides read-only access to the data. To get read-write access, use
    /// [`object_mut`].
    ///
    /// [`object_mut`]: crate::repo::key::KeyRepo::object_mut
    pub fn object<Q>(&self, key: &Q) -> Option<ReadOnlyObject>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = match self.objects.get(key) {
            Some(handle) => handle,
            None => return None,
        };
        Some(ReadOnlyObject::new(&self.state, handle))
    }

    /// Return an `Object` for reading and writing the object with the given `key`.
    ///
    /// This returns `None` if there is no object with the given `key` in the repository.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// [`object`].
    ///
    /// [`object`]: crate::repo::key::KeyRepo::object
    pub fn object_mut<Q>(&mut self, key: &Q) -> Option<Object>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = match self.objects.get_mut(key) {
            Some(handle) => handle,
            None => return None,
        };
        Some(Object::new(&mut self.state, handle))
    }

    /// Return an iterator over all the keys of objects in this repository.
    pub fn keys<'a>(&'a self) -> impl Iterator<Item = &'a K> + 'a {
        self.objects.keys()
    }

    /// Copy the object at `source` to `dest`.
    ///
    /// If another object already exists at `dest`, it is replaced.
    ///
    /// This returns `true` if the object was copied or `false` if there was no object at `source`.
    ///
    /// This is a cheap operation which does not require copying the bytes in the object.
    pub fn copy<Q>(&mut self, source: &Q, dest: K) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let source_handle = match self.objects.get(source) {
            Some(handle) => handle,
            None => return false,
        };

        self.remove(&dest);

        let dest_handle = ObjectHandle {
            id: self.handle_table.next(),
            chunks: source_handle.chunks.clone(),
        };

        // Update the chunk map to include the new handle in the list of references for each chunk.
        for chunk in &dest_handle.chunks {
            let chunk_info = self
                .state
                .chunks
                .get_mut(chunk)
                .expect("This chunk was not found in the repository.");
            chunk_info.references.insert(dest_handle.id);
        }

        self.objects.insert(dest, dest_handle);

        true
    }

    /// Return a list of blocks in the data store excluding those used to store metadata.
    fn list_data_blocks(&self) -> crate::Result<Vec<Uuid>> {
        let all_blocks = self
            .state
            .store
            .lock()
            .unwrap()
            .list_blocks()
            .map_err(|error| crate::Error::Store(error))?;

        Ok(all_blocks
            .iter()
            .copied()
            .filter(|id| {
                *id != METADATA_BLOCK_ID
                    && *id != VERSION_BLOCK_ID
                    && *id != self.state.metadata.header_id
            })
            .collect())
    }

    /// Write the map of objects for the current instance to the data store.
    pub(super) fn write_object_map(&mut self) -> crate::Result<()> {
        let handle = self
            .instances
            .entry(self.instance_id)
            .or_insert_with(|| ObjectHandle {
                id: self.handle_table.next(),
                chunks: Vec::new(),
            });
        let mut object = Object::new(&mut self.state, handle);
        object.serialize(&self.objects)
    }

    /// Read the object map for the current instance and modify the repository in-place.
    ///
    /// This does not write the object map for the old instance first. To do that, use
    /// `write_object_map`.
    ///
    /// This does not commit or roll back changes.
    pub(super) fn read_object_map(&mut self) -> crate::Result<()> {
        self.objects = match self.instances.get_mut(&self.instance_id) {
            Some(handle) => {
                let mut instance_object = Object::new(&mut state, handle);
                instance_object.deserialize()?
            }
            None => HashMap::new(),
        };
        Ok(())
    }

    /// Read the object map for the given `instance_id` and return a new `KeyRepo`.
    ///
    /// This does not write the object map for the old instance first. To do that, use
    /// `write_object_map`.
    ///
    /// This does not commit or roll back changes.
    pub(super) fn change_object_map<Q: Key>(
        mut self,
        instance_id: Uuid,
    ) -> crate::Result<KeyRepo<Q>> {
        // Read and deserialize the map of object handles for the new instance.
        let new_objects: HashMap<Q, ObjectHandle> = match self.instances.get_mut(&instance_id) {
            Some(handle) => {
                let mut instance_object = Object::new(&mut state, handle);
                instance_object.deserialize()?
            }
            None => HashMap::new(),
        };

        Ok(KeyRepo {
            state: self.state,
            instance_id,
            objects: new_objects,
            instances: self.instances,
            handle_table: self.handle_table,
            transaction_id: self.transaction_id,
        })
    }

    /// Atomically encode and write the given serialized `header` to the data store.
    fn write_serialized_header(&mut self, serialized_header: &[u8]) -> crate::Result<()> {
        // Encode the serialized header.
        let encoded_header = self.state.encode_data(serialized_header)?;

        // Write the new header to a new block.
        let header_id = Uuid::new_v4();
        self.state
            .store
            .lock()
            .unwrap()
            .write_block(header_id, encoded_header.as_slice())
            .map_err(|error| crate::Error::Store(error))?;
        self.state.metadata.header_id = header_id;

        // Atomically write the new repository metadata containing the new header ID.
        let serialized_metadata =
            to_vec(&self.state.metadata).expect("Could not serialize repository metadata.");
        self.state
            .store
            .lock()
            .unwrap()
            .write_block(METADATA_BLOCK_ID, &serialized_metadata)
            .map_err(|error| crate::Error::Store(error))
    }

    /// Return a cloned `Header` representing the current state of the repository.
    fn clone_header(&self) -> Header {
        Header {
            chunks: self.state.chunks.clone(),
            packs: self.state.packs.clone(),
            instances: self.instances.clone(),
            handle_table: self.handle_table.clone(),
        }
    }

    /// Return a serialized `Header` representing the current state of the repository.
    ///
    /// The returned data is not encoded.
    fn serialize_header(&mut self) -> Vec<u8> {
        // Temporarily replace the values in the repository which need to be serialized so we can
        // put them into the `Header`. This avoids the need to clone them. We'll put them back
        // later.
        let header = Header {
            chunks: mem::replace(&mut self.state.chunks, HashMap::new()),
            packs: mem::replace(&mut self.state.packs, HashMap::new()),
            instances: mem::replace(&mut self.instances, HashMap::new()),
            handle_table: mem::replace(&mut self.handle_table, IdTable::new()),
        };

        // Serialize the header so we can write it to the data store.
        let serialized_header =
            to_vec(&header).expect("Could not serialize the repository header.");

        // Unpack the values from the `Header` and put them back where they originally were.
        let Header {
            chunks,
            packs,
            instances,
            handle_table,
        } = header;
        self.state.chunks = chunks;
        self.state.packs = packs;
        self.instances = instances;
        self.handle_table = handle_table;

        serialized_header
    }

    /// Atomically restore the repository's state from the given `header`.
    ///
    /// This restores the state of the repository using the data in the given `header` and then
    /// reads the object map for the current instance from the data store.
    ///
    /// If this returns `Ok`, the repository's state has been restored. If this returns `Err`, the
    /// repository is unchanged.
    fn restore_header(&mut self, header: Header) -> crate::Result<()> {
        // We need to restore the repository state before we can read the object map.
        let old_chunks = mem::replace(&mut self.state.chunks, header.chunks);
        let old_packs = mem::replace(&mut self.state.packs, header.packs);
        let old_instances = mem::replace(&mut self.instances, header.instances);
        let old_handle_table = mem::replace(&mut self.handle_table, header.handle_table);

        let result = self.read_object_map();

        // If restoring the object map failed, we need to undo the changes we made to the repository
        // state so that the repository is unchanged.
        if result.is_err() {
            self.state.chunks = old_chunks;
            self.state.packs = old_packs;
            self.instances = old_instances;
            self.handle_table = old_handle_table;
        }

        result
    }

    /// Commit changes which have been made to the repository.
    ///
    /// No changes are saved persistently until this method is called.
    ///
    /// If this method returns `Ok`, changes have been committed. If this method returns `Err`,
    /// changes have not been committed.
    ///
    /// If changes are committed, this method invalidates all savepoints which are associated with
    /// this repository.
    ///
    /// To reclaim space from deleted objects in the backing data store, you must call [`clean`]
    /// after changes are committed.
    ///
    /// This method commits changes for all instances of the repository.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`clean`]: crate::repo::key::KeyRepo::clean
    pub fn commit(&mut self) -> crate::Result<()> {
        // Write the map of objects for the current instance.
        self.write_object_map()?;

        // Serialize the header.
        let serialized_header = self.serialize_header();

        // Write the serialized header to the data store, atomically completing the commit. If this
        // completes successfully, changes have been committed and this method MUST return `Ok`.
        self.write_serialized_header(serialized_header.as_slice())?;

        // Now that the commit has succeeded, we must invalidate all savepoints associated with this
        // repository.
        self.transaction_id = Arc::new(Uuid::new_v4());

        Ok(())
    }

    /// Roll back all changes made since the last commit.
    ///
    /// Uncommitted changes in repository are automatically rolled back when the repository is
    /// dropped. This method can be used to manually roll back changes without dropping and
    /// re-opening the repository.
    ///
    /// If this method returns `Ok`, changes have been rolled back. If this method returns `Err`,
    /// the repository is unchanged.
    ///
    /// This method rolls back changes for all instances of the repository.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn rollback(&mut self) -> crate::Result<()> {
        // Read the header from the previous commit from the data store.
        let encoded_header = self
            .state
            .store
            .lock()
            .unwrap()
            .read_block(self.state.metadata.header_id)
            .map_err(|error| crate::Error::Store(error))?
            .ok_or(crate::Error::Corrupt)?;
        let serialized_header = self.state.decode_data(encoded_header.as_slice())?;
        let header: Header =
            from_read(serialized_header.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // Atomically restore from the deserialized header.
        self.restore_header(header)
    }

    /// Create a new `Savepoint` representing the current state of the repository.
    ///
    /// You can restore the repository to this savepoint using [`restore`].
    ///
    /// Creating a savepoint does not commit changes to the repository; if the repository is
    /// dropped, it will revert to the previous commit and not the most recent savepoint.
    ///
    /// See [`Savepoint`] for details.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`restore`]: crate::repo::key::KeyRepo::restore
    /// [`Savepoint`]: crate::repo::Savepoint
    pub fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.write_object_map()?;

        Ok(Savepoint {
            header: Arc::new(self.clone_header()),
            transaction_id: Arc::downgrade(&self.transaction_id),
        })
    }

    /// Restore the repository to the given `savepoint`.
    ///
    /// This method functions similarly to [`rollback`], but instead of restoring the repository to
    /// the previous commit, it restores the repository to the given `savepoint`.
    ///
    /// If this method returns `Ok`, the repository has been restored. If this method returns `Err`,
    /// the repository is unchanged.
    ///
    /// This method affects all instances of the repository.
    ///
    /// See [`Savepoint`] for details.
    ///
    /// # Examples
    /// This example demonstrates restoring from a savepoint to undo a change to the repository.
    /// ```
    /// # use std::io::Write;
    /// # use acid_store::store::MemoryConfig;
    /// # use acid_store::repo::{OpenOptions, OpenMode, key::KeyRepo};
    /// #
    /// # let mut repo: KeyRepo<String> = OpenOptions::new()
    /// #     .mode(OpenMode::CreateNew)
    /// #     .open(&MemoryConfig::new())
    /// #     .unwrap();
    /// // Create a new savepoint.
    /// let savepoint = repo.savepoint().unwrap();
    ///
    /// // Write data to the repository.
    /// let mut object = repo.insert(String::from("test"));
    /// object.write_all(b"Some data").unwrap();
    /// object.flush().unwrap();
    /// drop(object);
    ///
    /// // Restore to the savepoint.
    /// repo.restore(&savepoint).unwrap();
    ///
    /// assert!(!repo.contains("test"));
    /// ```
    ///
    /// # Errors
    /// - `Error::NotFound`: The given savepoint is not associated with this repository.
    /// - `Error::InvalidSavepoint`: The given savepoint is invalid.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`rollback`]: crate::repo::key::KeyRepo::rollback
    /// [`Savepoint`]: crate::repo::Savepoint
    pub fn restore(&mut self, savepoint: &Savepoint) -> crate::Result<()> {
        match savepoint.transaction_id.upgrade() {
            None => return Err(crate::Error::InvalidSavepoint),
            Some(transaction_id) if transaction_id != self.transaction_id => {
                return Err(crate::Error::NotFound)
            }
            _ => (),
        }

        // Clone the repository header itself, not the pointer.
        let header = (*savepoint.header).clone();

        // Atomically restore from the header.
        self.restore_header(header)
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// When data in a repository is deleted, the space is not reclaimed in the backing data store
    /// until those changes are committed and this method is called.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn clean(&mut self) -> crate::Result<()> {
        // Read the header from the previous commit.
        let encoded_header = self
            .state
            .store
            .lock()
            .unwrap()
            .read_block(self.state.metadata.header_id)
            .map_err(|error| crate::Error::Store(error))?
            .ok_or(crate::Error::Corrupt)?;
        let serialized_header = self.state.decode_data(encoded_header.as_slice())?;
        let previous_header: Header =
            from_read(serialized_header.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // We need to find the set of blocks which are either currently referenced by the repository
        // or were referenced after the previous commit. It's important that we don't clean up
        // blocks which were referenced after the previous commit because that would make it
        // impossible to roll back changes, and this method may be called before the repository is
        // committed.
        let mut referenced_blocks = self
            .state
            .chunks
            .values()
            .map(|info| info.block_id)
            .collect::<HashSet<_>>();
        let previous_referenced_blocks = previous_header.chunks.values().map(|info| info.block_id);
        referenced_blocks.extend(previous_referenced_blocks);

        // Remove all blocks from the data store which are unreferenced.
        match &self.state.metadata.config.packing {
            Packing::None => {
                // When packing is disabled, we can just remove the unreferenced blocks from the
                // data store directly.
                let block_ids = self.list_data_blocks()?;

                let mut store = self.state.store.lock().unwrap();
                for block_id in block_ids {
                    if !referenced_blocks.contains(&block_id) {
                        store
                            .remove_block(block_id)
                            .map_err(|error| crate::Error::Store(error))?;
                    }
                }
            }
            Packing::Fixed(_) => {
                // When packing is enabled, we need to repack the packs which contain unreferenced
                // blocks.

                // Get an iterator of block IDs and the list of packs they're contained in.
                let blocks_to_packs = self.state.packs.iter().chain(previous_header.packs.iter());

                // Get a map of pack IDs to the set of blocks contained in them.
                let mut packs_to_blocks = HashMap::new();
                for (block_id, index_list) in blocks_to_packs {
                    for pack_index in index_list {
                        packs_to_blocks
                            .entry(pack_index.id)
                            .or_insert_with(HashSet::new)
                            .insert(*block_id);
                    }
                }

                // The list of IDs of packs which contain at least one unreferenced block.
                let mut packs_to_remove = Vec::new();

                // The list of blocks which need to be repacked. These are referenced blocks which
                // are contained in packs which contain at least one unreferenced block.
                let mut blocks_to_repack = Vec::new();

                // Iterate over the IDs of packs which are contained in the data store.
                for pack_id in self.list_data_blocks()? {
                    match packs_to_blocks.get(&pack_id) {
                        Some(contained_blocks) => {
                            let contains_unreferenced_blocks = contained_blocks
                                .iter()
                                .any(|block_id| !referenced_blocks.contains(block_id));
                            if contains_unreferenced_blocks {
                                let contained_referenced_blocks =
                                    contained_blocks.intersection(&referenced_blocks).copied();
                                packs_to_remove.push(pack_id);
                                blocks_to_repack.extend(contained_referenced_blocks);
                            }
                        }
                        // This pack does not contain any blocks that we know about. We can remove
                        // it.
                        None => packs_to_remove.push(pack_id),
                    }
                }

                // For each block that needs repacking, read it from its current pack and write it
                // to a new one.
                {
                    let mut store_state = StoreState::new();
                    let mut store_writer = StoreWriter::new(&mut self.state, &mut store_state);
                    for block_id in blocks_to_repack {
                        let block_data = store_writer.read_block(block_id)?;
                        store_writer.write_block(block_id, block_data.as_slice())?;
                    }
                }

                // Once all the referenced blocks have been written to new packs, remove the old
                // packs from the data store.
                {
                    let mut store = self.state.store.lock().unwrap();
                    for pack_id in packs_to_remove {
                        store
                            .remove_block(pack_id)
                            .map_err(|error| crate::Error::Store(error))?;
                    }
                }

                // Once old packs have been removed from the data store, all unreferenced blocks
                // have been removed from the data store. At this point, we can remove those
                // blocks from the pack map. Because block IDs are random UUIDs and are
                // never reused, having nonexistent blocks in the pack map won't cause problems.
                // However, it may cause unnecessary repacking on subsequent calls to this method
                // and it will consume additional memory. For this reason, it's beneficial to remove
                // nonexistent blocks from the pack map, but if this method returns early or panics
                // before this step can complete, the repository will not be in an inconsistent
                // state.
                self.state
                    .packs
                    .retain(|block_id, _| referenced_blocks.contains(block_id));

                // Next we need to write the updated pack map to the data store. To do this, we have
                // to write the entire header. Because this method does not commit any changes, it's
                // important that we write the previous header, changing only the pack map.
                {
                    let mut previous_header = previous_header;

                    // Temporarily move the pack map into the previous header just so that we can
                    // serialize it. Once we're done, move it back. This avoids needing the clone
                    // the pack map.
                    previous_header.packs = mem::replace(&mut self.state.packs, HashMap::new());
                    let serialized_header = to_vec(&previous_header)
                        .expect("Could not serialize the repository header.");
                    mem::swap(&mut previous_header.packs, &mut self.state.packs);
                    drop(previous_header);

                    // Encode the serialized header and write it to the data store.
                    let encoded_header = self.state.encode_data(serialized_header.as_slice())?;
                    self.write_serialized_header(encoded_header.as_slice())?;
                }
            }
        }

        Ok(())
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// This does not delete data from other instances of the repository.
    ///
    /// This does not commit changes to the repository.
    ///
    /// No data is reclaimed in the backing data store until changes are committed and [`clean`] is
    /// called.
    ///
    /// [`clean`]: crate::repo::key::KeyRepo::clean
    pub fn clear_instance(&mut self) {
        for handle in self.objects.values() {
            self.remove_handle(handle);
        }
        self.objects.clear();
    }

    /// Delete all data in all instances of the repository.
    ///
    /// This does not commit changes to the repository.
    ///
    /// No data is reclaimed in the backing data store until changes are committed and [`clean`] is
    /// called.
    ///
    /// [`clean`]: crate::repo::key::KeyRepo::clean
    pub fn clear_repo(&mut self) {
        // Because this method cannot return early, it doesn't matter which order we do these in.
        self.handle_table = IdTable::new();
        self.state.chunks.clear();
        self.state.packs.clear();
        self.instances.clear();
        self.objects.clear();
    }

    /// Verify the integrity of all the data in the current instance of the repository.
    ///
    /// This returns the set of keys of objects in the current instance which are corrupt.
    ///
    /// If you just need to verify the integrity of one object, [`Object::verify`] is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`Object::verify`]: crate::repo::Object::verify
    pub fn verify(&self) -> crate::Result<HashSet<&K>> {
        let mut corrupt_chunks = HashSet::new();
        let expected_chunks = self.state.chunks.keys().copied().collect::<Vec<_>>();

        // Get the set of hashes of chunks which are corrupt.
        let mut store_state = StoreState::new();
        let mut store_reader = StoreReader::new(&self.state, &mut store_state);
        for chunk in expected_chunks {
            match store_reader.read_chunk(chunk) {
                Ok(data) => {
                    if data.len() != chunk.size as usize || chunk_hash(&data) != chunk.hash {
                        corrupt_chunks.insert(chunk.hash);
                    }
                }
                Err(crate::Error::InvalidData) => {
                    // Ciphertext verification failed. No need to check the hash.
                    corrupt_chunks.insert(chunk.hash);
                }
                Err(error) => return Err(error),
            };
        }

        // If there are no corrupt chunks, there are no corrupt objects.
        if corrupt_chunks.is_empty() {
            return Ok(HashSet::new());
        }

        let mut corrupt_keys = HashSet::new();
        for (key, handle) in &self.objects {
            for chunk in &handle.chunks {
                // If any one of the object's chunks is corrupt, the object is corrupt.
                if corrupt_chunks.contains(&chunk.hash) {
                    corrupt_keys.insert(key);
                    break;
                }
            }
        }

        Ok(corrupt_keys)
    }

    /// Change the password for this repository.
    ///
    /// This replaces the existing password with `new_password`. Changing the password does not
    /// require re-encrypting any data. The change does not take effect until [`commit`] is called.
    /// If encryption is disabled, this method does nothing.
    ///
    /// [`commit`]: crate::repo::key::KeyRepo::commit
    pub fn change_password(&mut self, new_password: &[u8]) {
        let salt = KeySalt::generate();
        let user_key = EncryptionKey::derive(
            new_password,
            &salt,
            self.state.metadata.config.encryption.key_size(),
            self.state.metadata.config.memory_limit,
            self.state.metadata.config.operations_limit,
        );

        let encrypted_master_key = self
            .state
            .metadata
            .config
            .encryption
            .encrypt(self.state.master_key.expose_secret(), &user_key);

        self.state.metadata.salt = salt;
        self.state.metadata.master_key = encrypted_master_key;
    }

    /// Return this repository's current instance ID.
    pub fn instance(&self) -> Uuid {
        self.instance_id
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.state.metadata.to_info()
    }
}
