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

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::mem;
use std::sync::{Arc, RwLock};

use hex_literal::hex;
use rmp_serde::{from_read, to_vec};
use secrecy::ExposeSecret;
use uuid::Uuid;

use crate::store::{BlockId, DataStore};

use super::chunk_store::{
    EncodeBlock, ReadBlock, ReadChunk, StoreReader, StoreState, StoreWriter, WriteBlock,
};
use super::commit::Commit;
use super::encryption::{Encryption, EncryptionKey, KeySalt, ResourceLimit};
use super::handle::{chunk_hash, HandleId, HandleIdTable, ObjectHandle, ObjectId};
use super::key::{Key, Keys};
use super::metadata::{Header, RepoInfo, RepoStats};
use super::object::Object;
use super::object_store::{ObjectReader, ObjectWriter};
use super::open_repo::OpenRepo;
use super::open_repo::VersionId;
use super::packing::Packing;
use super::savepoint::{KeyRestore, RestoreSavepoint, Savepoint};
use super::state::{InstanceId, InstanceInfo, ObjectState, RepoState};

/// The block ID of the block which stores the repository metadata.
pub(super) const METADATA_BLOCK_ID: BlockId = BlockId::new(Uuid::from_bytes(hex!(
    "8691d360 29c6 11ea 8bc1 2fc8cfe66f33"
)));

/// The block ID of the block which stores the repository format version.
pub(super) const VERSION_BLOCK_ID: BlockId = BlockId::new(Uuid::from_bytes(hex!(
    "cbf28b1c 3550 11ea 8cb0 87d7a14efe10"
)));

/// Return a list of blocks in the data store excluding those used to store metadata.
fn list_data_blocks(state: &RepoState) -> crate::Result<Vec<BlockId>> {
    let all_blocks = state
        .store
        .lock()
        .unwrap()
        .list_blocks()
        .map_err(crate::Error::Store)?;

    Ok(all_blocks
        .iter()
        .copied()
        .filter(|id| {
            *id != METADATA_BLOCK_ID && *id != VERSION_BLOCK_ID && *id != state.metadata.header_id
        })
        .collect())
}

/// An object store which maps keys to seekable binary blobs.
///
/// See [`crate::repo::key`] for more information.
#[derive(Debug)]
pub struct KeyRepo<K: Key> {
    /// The state for this repository.
    pub(super) state: Arc<RwLock<RepoState>>,

    /// The instance ID of this repository instance.
    pub(super) instance_id: InstanceId,

    /// A map of object keys to their object handles for the current instance.
    pub(super) objects: HashMap<K, Arc<RwLock<ObjectHandle>>>,

    /// A map of instance IDs to information about those instances.
    pub(super) instances: HashMap<InstanceId, InstanceInfo>,

    /// A table of unique IDs of existing handles.
    ///
    /// We use this to determine whether a handle is contained in the repository without actually
    /// storing it.
    pub(super) handle_table: HandleIdTable,

    /// The unique ID for the current transaction.
    ///
    /// This ID changes each time the repository is opened or committed. It is used to invalidate
    /// savepoints.
    pub(super) transaction_id: Arc<Uuid>,
}

impl<K: Key> OpenRepo for KeyRepo<K> {
    type Key = K;

    const VERSION_ID: VersionId = VersionId::new(Uuid::from_bytes(hex!(
        "989a6a76 9d8b 46b7 9c05 d1c5e0d9471a"
    )));

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
    /// Return the `object_id` for the object with the given `handle_id`.
    fn object_id(&self, handle_id: HandleId) -> ObjectId {
        let state = self.state.read().unwrap();
        let repo_id = state.metadata.id;
        ObjectId::new(repo_id, self.instance_id, handle_id)
    }

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
        let handle_id = self.handle_table.next();
        let object_id = self.object_id(handle_id);
        let handle = ObjectHandle {
            id: handle_id,
            extents: Vec::new(),
        };
        assert!(!self.objects.contains_key(&key));
        let handle = self
            .objects
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(handle)));
        Object::new(&self.state, handle, object_id)
    }

    /// Remove the given object `handle` from the repository.
    fn remove_handle(&mut self, handle: &ObjectHandle) {
        let mut state = self.state.write().unwrap();
        for chunk in handle.chunks() {
            let chunk_info = state
                .chunks
                .get_mut(&chunk)
                .expect("This chunk was not found in the repository.");
            chunk_info.references.remove(&handle.id);
            if chunk_info.references.is_empty() {
                state.chunks.remove(&chunk);
            }
        }
        self.handle_table.recycle(handle.id);
    }

    /// Remove the object with the given `key` from the repository.
    ///
    /// This returns `true` if the object was removed or `false` if it didn't exist.
    ///
    /// The space used by the given object isn't reclaimed in the backing data store until changes
    /// are committed and [`Commit::clean`] is called.
    ///
    /// [`Commit::clean`]: crate::repo::Commit::clean
    pub fn remove<Q>(&mut self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = match self.objects.remove(key) {
            Some(handle) => handle,
            None => return false,
        };
        let handle_guard = handle.read().unwrap();
        self.remove_handle(&handle_guard);
        true
    }

    /// Return an object for reading and writing the object with the given `key`.
    ///
    /// This returns `None` if there is no object with the given `key` in the repository.
    pub fn object<Q>(&self, key: &Q) -> Option<Object>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let handle = self.objects.get(key)?;
        let handle_id = handle.read().unwrap().id;
        Some(Object::new(&self.state, handle, self.object_id(handle_id)))
    }

    /// Return an iterator over all the keys of objects in this repository.
    pub fn keys(&self) -> Keys<K> {
        Keys(self.objects.keys())
    }

    /// Copy the object at `source` to `dest`.
    ///
    /// If another object already exists at `dest`, it is replaced.
    ///
    /// This returns `true` if the object was copied or `false` if there was no object at source.
    ///
    /// This is a cheap operation which does not require copying the bytes in the object.
    pub fn copy<Q>(&mut self, source: &Q, dest: K) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let source_chunks = match self.objects.get(source) {
            Some(handle) => handle.read().unwrap().extents.clone(),
            None => return false,
        };

        self.remove(dest.borrow());

        let dest_handle = ObjectHandle {
            id: self.handle_table.next(),
            extents: source_chunks,
        };

        // Update the chunk map to include the new handle in the list of references for each chunk.
        let mut state = self.state.write().unwrap();
        for chunk in dest_handle.chunks() {
            let chunk_info = state
                .chunks
                .get_mut(&chunk)
                .expect("This chunk was not found in the repository.");
            chunk_info.references.insert(dest_handle.id);
        }

        self.objects
            .insert(dest, Arc::new(RwLock::new(dest_handle)));

        true
    }

    /// Write the map of objects for the current instance to the data store.
    pub(super) fn write_object_map(&mut self) -> crate::Result<()> {
        let mut state = self.state.write().unwrap();

        let handle = &mut self
            .instances
            .get_mut(&self.instance_id)
            .expect("There is no instance with the given ID.")
            .objects;

        let mut object_state = ObjectState::new(state.metadata.config.chunking.to_chunker());
        let mut writer = ObjectWriter::new(&mut state, &mut object_state, handle);
        writer.serialize(&self.objects)
    }

    /// Read the object map for the current instance from the data store and return it.
    ///
    /// This does not write the object map for the old instance first. To do that, use
    /// `write_object_map`.
    ///
    /// This does not commit or roll back changes.
    pub(super) fn read_object_map(&self) -> crate::Result<HashMap<K, Arc<RwLock<ObjectHandle>>>> {
        let state = self.state.read().unwrap();
        match self.instances.get(&self.instance_id) {
            Some(instance_info) => {
                let mut object_state =
                    ObjectState::new(state.metadata.config.chunking.to_chunker());
                let mut reader =
                    ObjectReader::new(&state, &mut object_state, &instance_info.objects);
                reader.deserialize()
            }
            None => {
                // If the current instance is not in the instance map, then this repository has not
                // been committed since it was created and an object map has not been written for
                // this instance.
                Ok(HashMap::new())
            }
        }
    }

    /// Set the current instance of the repository.
    ///
    /// This does not write the object map for the current instance before switching to the new
    /// instance.
    pub(super) fn change_instance<R: OpenRepo>(
        mut self,
        instance_id: InstanceId,
    ) -> crate::Result<R> {
        let is_new_instance = !self.instances.contains_key(&instance_id);

        let new_objects = if is_new_instance {
            // Create the object handle for the object which will store the object map for the new
            // instance.
            let mut handle = ObjectHandle {
                id: self.handle_table.next(),
                extents: Vec::new(),
            };

            // Because this is a new instance, we return an empty object map.
            let objects = HashMap::<R::Key, Arc<RwLock<ObjectHandle>>>::new();

            // Write an empty object map to the object.
            let mut state = self.state.write().unwrap();
            let mut object_state = ObjectState::new(state.metadata.config.chunking.to_chunker());
            let mut writer = ObjectWriter::new(&mut state, &mut object_state, &mut handle);
            writer.serialize(&objects)?;

            // Insert the instance info into the instance map.
            let instance_info = InstanceInfo {
                version_id: R::VERSION_ID,
                objects: handle,
            };
            self.instances.insert(instance_id, instance_info);

            objects
        } else {
            let instance_info = self.instances.get_mut(&instance_id).unwrap();

            if instance_info.version_id != R::VERSION_ID {
                return Err(crate::Error::UnsupportedRepo);
            }

            // Deserialize the object map for this instance.
            let state = self.state.read().unwrap();
            let mut object_state = ObjectState::new(state.metadata.config.chunking.to_chunker());
            let mut reader = ObjectReader::new(&state, &mut object_state, &instance_info.objects);
            reader.deserialize()?
        };

        let repo = KeyRepo {
            state: self.state,
            instance_id,
            objects: new_objects,
            instances: self.instances,
            handle_table: self.handle_table,
            transaction_id: self.transaction_id,
        };

        if is_new_instance {
            R::create_repo(repo)
        } else {
            R::open_repo(repo)
        }
    }

    /// Atomically encode and write the given serialized `header` to the data store.
    fn write_serialized_header(&mut self, serialized_header: &[u8]) -> crate::Result<()> {
        let mut state = self.state.write().unwrap();
        // Encode the serialized header.
        let encoded_header = state.encode_data(serialized_header)?;

        // Write the new header to a new block.
        let header_id = Uuid::new_v4().into();
        state
            .store
            .lock()
            .unwrap()
            .write_block(header_id, encoded_header.as_slice())
            .map_err(crate::Error::Store)?;
        state.metadata.header_id = header_id;

        // Atomically write the new repository metadata containing the new header ID.
        let serialized_metadata =
            to_vec(&state.metadata).expect("Could not serialize repository metadata.");
        state
            .store
            .lock()
            .unwrap()
            .write_block(METADATA_BLOCK_ID, &serialized_metadata)
            .map_err(crate::Error::Store)?;
        Ok(())
    }

    /// Return a cloned `Header` representing the current state of the repository.
    fn clone_header(&self) -> Header {
        let state = self.state.read().unwrap();
        Header {
            chunks: state.chunks.clone(),
            packs: state.packs.clone(),
            instances: self.instances.clone(),
            handle_table: self.handle_table.clone(),
        }
    }

    /// Return a serialized `Header` representing the current state of the repository.
    ///
    /// The returned data is not encoded.
    fn serialize_header(&mut self) -> Vec<u8> {
        let mut state = self.state.write().unwrap();
        // Temporarily replace the values in the repository which need to be serialized so we can
        // put them into the `Header`. This avoids the need to clone them. We'll put them back
        // later.
        let header = Header {
            chunks: std::mem::take(&mut state.chunks),
            packs: std::mem::take(&mut state.packs),
            instances: std::mem::take(&mut self.instances),
            handle_table: std::mem::take(&mut self.handle_table),
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
        state.chunks = chunks;
        state.packs = packs;
        self.instances = instances;
        self.handle_table = handle_table;

        serialized_header
    }

    /// Replace the repository header with `header` and return the old one.
    fn replace_header(&mut self, header: Header) -> Header {
        let mut state = self.state.write().unwrap();
        let old_chunks = mem::replace(&mut state.chunks, header.chunks);
        let old_packs = mem::replace(&mut state.packs, header.packs);
        let old_instances = mem::replace(&mut self.instances, header.instances);
        let old_handle_table = mem::replace(&mut self.handle_table, header.handle_table);
        Header {
            chunks: old_chunks,
            packs: old_packs,
            instances: old_instances,
            handle_table: old_handle_table,
        }
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
        let old_header = self.replace_header(header);

        // Restore the object map from the old header.
        match self.read_object_map() {
            Ok(objects) => {
                self.objects = objects;
                Ok(())
            }
            Err(error) => {
                self.replace_header(old_header);
                Err(error)
            }
        }
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
        let state = self.state.read().unwrap();

        let mut corrupt_chunks = HashSet::new();
        let expected_chunks = state.chunks.keys().copied().collect::<Vec<_>>();

        // Get the set of hashes of chunks which are corrupt.
        let mut store_state = StoreState::new();
        let mut store_reader = StoreReader::new(&state, &mut store_state);
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
            for chunk in handle.read().unwrap().chunks() {
                // If any one of the object's chunks is corrupt, the object is corrupt.
                if corrupt_chunks.contains(&chunk.hash) {
                    corrupt_keys.insert(key);
                    break;
                }
            }
        }

        Ok(corrupt_keys)
    }

    /// Delete all data in the current instance of the repository.
    ///
    /// This does not delete data from other instances of the repository.
    ///
    /// This does not commit changes to the repository.
    ///
    /// No data is reclaimed in the backing data store until changes are committed and
    /// [`Commit::clean`] is called.
    ///
    /// [`Commit::clean`]: crate::repo::Commit::clean
    pub fn clear_instance(&mut self) {
        let handles = self
            .objects
            .drain()
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();
        for handle in handles {
            self.remove_handle(&*handle.read().unwrap());
        }
    }

    /// Change the password for this repository.
    ///
    /// This replaces the existing password with `new_password`. This also accepts the
    /// `memory_limit` and the `operations_limit`, which affect the amount of memory and the number
    /// of computations respectively which will be used by the key derivation function.
    ///
    /// Changing the password does not require re-encrypting any data. The change does not take
    /// effect until [`Commit::commit`] is called.
    ///
    /// If encryption is disabled, this method does nothing.
    ///
    /// [`Commit::commit`]: crate::repo::Commit::commit
    pub fn change_password(
        &mut self,
        new_password: &[u8],
        memory_limit: ResourceLimit,
        operations_limit: ResourceLimit,
    ) {
        let mut state = self.state.write().unwrap();

        if state.metadata.config.encryption == Encryption::None {
            return;
        }

        let salt = KeySalt::generate();
        let user_key = EncryptionKey::derive(
            new_password,
            &salt,
            state.metadata.config.encryption.key_size(),
            memory_limit,
            operations_limit,
        );

        let encrypted_master_key = state
            .metadata
            .config
            .encryption
            .encrypt(state.master_key.expose_secret(), &user_key);

        state.metadata.salt = salt;
        state.metadata.master_key = encrypted_master_key;
        state.metadata.config.memory_limit = memory_limit;
        state.metadata.config.operations_limit = operations_limit;
    }

    /// Return this repository's current instance ID.
    pub fn instance(&self) -> InstanceId {
        self.instance_id
    }

    /// Compute statistics about the repository.
    ///
    /// The returned `RepoStats` represents the contents of the repository at the time this method
    /// was called. It is not updated when the repository is modified.
    pub fn stats(&self) -> RepoStats {
        let mut apparent_size = 0u64;
        let mut actual_size = 0u64;
        let mut repo_size = 0u64;

        // The set of object handle IDs of objects in the current instance.
        let mut current_instance_handles = HashSet::new();

        // The set of object handle IDs of objects which store metadata and shouldn't count towards
        // the `repo_size`.
        let metadata_handles = self
            .instances
            .values()
            .map(|info| info.objects.id)
            .collect::<HashSet<_>>();

        for handle_lock in self.objects.values() {
            let handle = handle_lock.read().unwrap();
            apparent_size += handle.size();
            current_instance_handles.insert(handle.id);
        }

        let state = self.state.read().unwrap();
        for (chunk, info) in state.chunks.iter() {
            // Only count object inserted by the user in the `repo_size`.
            if !info.references.is_subset(&metadata_handles) {
                repo_size += chunk.size as u64;
            }

            if !info.references.is_disjoint(&current_instance_handles) {
                actual_size += chunk.size as u64;
            }
        }

        RepoStats {
            apparent_size,
            actual_size,
            repo_size,
        }
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.state.read().unwrap().metadata.to_info()
    }
}

impl<K: Key> RestoreSavepoint for KeyRepo<K> {
    type Restore = KeyRestore<K>;

    fn savepoint(&mut self) -> crate::Result<Savepoint> {
        self.write_object_map()?;

        Ok(Savepoint {
            header: Arc::new(self.clone_header()),
            transaction_id: Arc::downgrade(&self.transaction_id),
        })
    }

    fn start_restore(&mut self, savepoint: &Savepoint) -> crate::Result<Self::Restore> {
        match savepoint.transaction_id.upgrade() {
            None => return Err(crate::Error::InvalidSavepoint),
            Some(transaction_id) if transaction_id != self.transaction_id => {
                return Err(crate::Error::InvalidSavepoint)
            }
            _ => (),
        }

        let old_header = self.replace_header((*savepoint.header).clone());

        match self.read_object_map() {
            Ok(objects) => Ok(KeyRestore {
                objects,
                header: self.replace_header(old_header),
                transaction_id: savepoint.transaction_id.clone(),
                instance_id: self.instance_id,
            }),
            Err(error) => {
                self.replace_header(old_header);
                Err(error)
            }
        }
    }

    fn finish_restore(&mut self, restore: Self::Restore) -> bool {
        match restore.transaction_id.upgrade() {
            None => return false,
            Some(transaction_id) if transaction_id != self.transaction_id => return false,
            _ => (),
        }

        if restore.instance_id != self.instance_id {
            return false;
        }

        self.replace_header(restore.header);
        self.objects = restore.objects;

        true
    }
}

impl<K: Key> Commit for KeyRepo<K> {
    fn commit(&mut self) -> crate::Result<()> {
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

    fn rollback(&mut self) -> crate::Result<()> {
        let state = self.state.read().unwrap();
        // Read the header from the previous commit from the data store.
        let encoded_header = state
            .store
            .lock()
            .unwrap()
            .read_block(state.metadata.header_id)
            .map_err(crate::Error::Store)?
            .ok_or(crate::Error::Corrupt)?;
        let serialized_header = state.decode_data(encoded_header.as_slice())?;
        let header: Header =
            from_read(serialized_header.as_slice()).map_err(|_| crate::Error::Corrupt)?;
        drop(state);

        // Atomically restore from the deserialized header.
        self.restore_header(header)
    }

    fn clean(&mut self) -> crate::Result<()> {
        let mut state = self.state.write().unwrap();

        // Read the header from the previous commit.
        let encoded_header = state
            .store
            .lock()
            .unwrap()
            .read_block(state.metadata.header_id)
            .map_err(crate::Error::Store)?
            .ok_or(crate::Error::Corrupt)?;
        let serialized_header = state.decode_data(encoded_header.as_slice())?;
        let previous_header: Header =
            from_read(serialized_header.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        // We need to find the set of blocks which are either currently referenced by the repository
        // or were referenced after the previous commit. It's important that we don't clean up
        // blocks which were referenced after the previous commit because that would make it
        // impossible to roll back changes, and this method may be called before the repository is
        // committed.
        let mut referenced_blocks = state
            .chunks
            .values()
            .map(|info| info.block_id)
            .collect::<HashSet<_>>();
        let previous_referenced_blocks = previous_header.chunks.values().map(|info| info.block_id);
        referenced_blocks.extend(previous_referenced_blocks);

        // Remove all blocks from the data store which are unreferenced.
        match &state.metadata.config.packing {
            Packing::None => {
                // When packing is disabled, we can just remove the unreferenced blocks from the
                // data store directly.
                let block_ids = list_data_blocks(&state)?;

                let mut store = state.store.lock().unwrap();
                for block_id in block_ids {
                    if !referenced_blocks.contains(&block_id) {
                        store.remove_block(block_id).map_err(crate::Error::Store)?;
                    }
                }
            }
            Packing::Fixed(_) => {
                // When packing is enabled, we need to repack the packs which contain unreferenced
                // blocks.

                // Get an iterator of block IDs and the list of packs they're contained in.
                let blocks_to_packs = state.packs.iter().chain(previous_header.packs.iter());

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
                for pack_id in list_data_blocks(&state)? {
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
                    let mut store_writer = StoreWriter::new(&mut state, &mut store_state);
                    for block_id in blocks_to_repack {
                        let block_data = store_writer.read_block(block_id)?;
                        store_writer.write_block(block_id, block_data.as_slice())?;
                    }
                }

                // Once all the referenced blocks have been written to new packs, remove the old
                // packs from the data store.
                {
                    let mut store = state.store.lock().unwrap();
                    for pack_id in packs_to_remove {
                        store.remove_block(pack_id).map_err(crate::Error::Store)?;
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
                state
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
                    previous_header.packs = std::mem::take(&mut state.packs);
                    let serialized_header = to_vec(&previous_header)
                        .expect("Could not serialize the repository header.");
                    mem::swap(&mut previous_header.packs, &mut state.packs);
                    drop(previous_header);

                    // Encode the serialized header and write it to the data store.
                    let encoded_header = state.encode_data(serialized_header.as_slice())?;
                    drop(state);
                    self.write_serialized_header(encoded_header.as_slice())?;
                }
            }
        }

        Ok(())
    }
}
