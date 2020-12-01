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

use std::collections::{HashMap, HashSet};
use std::mem;

use hex_literal::hex;
use rmp_serde::{from_read, to_vec};
use secrecy::ExposeSecret;
use uuid::Uuid;

use crate::repo::ConvertRepo;
use crate::store::DataStore;

use super::chunk_store::EncodeBlock;
use super::chunk_store::{ChunkEncoder, ReadChunk, StoreReader};
use super::encryption::{EncryptionKey, KeySalt};
use super::id_table::IdTable;
use super::metadata::{Header, RepoInfo, RepoMetadata};
use super::object::{chunk_hash, Object, ObjectHandle, ReadOnlyObject};
use super::report::IntegrityReport;
use super::state::RepoState;

/// The block ID of the block which stores the repository metadata.
pub(super) const METADATA_BLOCK_ID: Uuid =
    Uuid::from_bytes(hex!("8691d360 29c6 11ea 8bc1 2fc8cfe66f33"));

/// The block ID of the block which stores the repository format version.
pub(super) const VERSION_BLOCK_ID: Uuid =
    Uuid::from_bytes(hex!("cbf28b1c 3550 11ea 8cb0 87d7a14efe10"));

/// A low-level repository type which provides more direct access to the underlying storage.
///
/// See [`crate::repo::object`] for more information.
#[derive(Debug)]
pub struct ObjectRepo {
    /// The state for this repository.
    pub(super) state: RepoState,

    /// The instance ID of this repository instance.
    pub(super) instance_id: Uuid,

    /// A map of handles of managed objects.
    ///
    /// This is a map of repository instance IDs to maps of managed object IDs to object handles.
    pub(super) managed: HashMap<Uuid, HashMap<Uuid, ObjectHandle>>,

    /// A table of unique IDs of existing handles.
    ///
    /// We use this to determine whether a handle is contained in the repository without actually
    /// storing it.
    pub(super) handle_table: IdTable,
}

impl ConvertRepo for ObjectRepo {
    fn from_repo(repository: ObjectRepo) -> crate::Result<Self>
    where
        Self: Sized,
    {
        Ok(repository)
    }

    fn into_repo(mut self) -> crate::Result<ObjectRepo> {
        self.rollback()?;
        Ok(self)
    }
}

impl ObjectRepo {
    /// Return whether there is an unmanaged object associated with `handle` in this repository.
    pub fn contains_unmanaged(&self, handle: &ObjectHandle) -> bool {
        handle.repo_id == self.state.metadata.id
            && handle.instance_id == self.instance_id
            && self.handle_table.contains(handle.handle_id)
    }

    /// Add a new unmanaged object to the repository and return its handle.
    pub fn add_unmanaged(&mut self) -> ObjectHandle {
        ObjectHandle {
            repo_id: self.state.metadata.id,
            instance_id: self.instance_id,
            handle_id: self.handle_table.next(),
            size: 0,
            chunks: Vec::new(),
        }
    }

    /// Remove all the data associated with `handle` from the repository.
    ///
    /// This returns `true` if the object was removed and `false` if there is no unmanaged object
    /// in the repository associated with `handle`.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    pub fn remove_unmanaged(&mut self, handle: &ObjectHandle) -> bool {
        if !self.contains_unmanaged(&handle) {
            return false;
        }

        for chunk in &handle.chunks {
            let chunk_info = self
                .state
                .chunks
                .get_mut(chunk)
                .expect("This chunk was not found in the repository.");
            chunk_info.references.remove(&handle.handle_id);
            if chunk_info.references.is_empty() {
                self.state.chunks.remove(chunk);
            }
        }

        self.handle_table.recycle(handle.handle_id);

        true
    }

    /// Return a `ReadOnlyObject` for reading the data associated with `handle`.
    ///
    /// This returns `None` if there is no unmanaged object in the repository associated with
    /// `handle`.
    ///
    /// The returned object provides read-only access to the data. To get read-write access, use
    /// `unmanaged_object_mut`.
    pub fn unmanaged_object<'a>(&'a self, handle: &'a ObjectHandle) -> Option<ReadOnlyObject<'a>> {
        if self.contains_unmanaged(handle) {
            Some(ReadOnlyObject::new(&self.state, handle))
        } else {
            None
        }
    }

    /// Return an `Object` for reading and writing the data associated with `handle`.
    ///
    /// This returns `None` if there is no unmanaged object in the repository associated with
    /// `handle`.
    ///
    /// This takes a mutable reference to `handle` because we need to update the `ObjectHandle` to
    /// point to the new data.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// `unmanaged_object`.
    pub fn unmanaged_object_mut<'a>(
        &'a mut self,
        handle: &'a mut ObjectHandle,
    ) -> Option<Object<'a>> {
        if !self.contains_unmanaged(handle) {
            return None;
        }

        // Update the `ObjectHandle::handle_id` of `handle`.
        //
        // If the user clones the `handle`, and tries to modify one of the clones by calling this
        // method, we could end up in a situation where we have two handles with the same handle ID
        // which reference different contents. Once the user calls `remove_unmanaged` with one of
        // the handles, the repository will remove that handle ID from the `handle_table` and
        // prevent the data associated with any clones from being removed.
        //
        // `ObjectHandle` does not implement `Clone` for this reason, but it could still happen
        // since `ObjectHandle` is serializable. As an added precaution in case this happens, we
        // change the handle's handle ID every time we modify it so that we never end up with two
        // different handles with the same ID.
        let old_handle_id = mem::replace(&mut handle.handle_id, self.handle_table.next());
        self.handle_table.recycle(old_handle_id);
        for chunk in &handle.chunks {
            let chunk_info = self
                .state
                .chunks
                .get_mut(chunk)
                .expect("This chunk was not found in the repository.");
            chunk_info.references.remove(&old_handle_id);
            chunk_info.references.insert(handle.handle_id);
        }

        Some(Object::new(&mut self.state, handle))
    }

    /// Create a new object with the same contents as `source` and return its handle.
    pub fn copy_unmanaged(&mut self, source: &ObjectHandle) -> ObjectHandle {
        let new_handle = ObjectHandle {
            repo_id: self.state.metadata.id,
            instance_id: self.instance_id,
            handle_id: self.handle_table.next(),
            size: source.size,
            chunks: source.chunks.clone(),
        };

        // Update the chunk map to include the new handle in the list of references for each chunk.
        for chunk in &new_handle.chunks {
            let chunk_info = self
                .state
                .chunks
                .get_mut(chunk)
                .expect("This chunk was not found in the repository.");
            chunk_info.references.insert(new_handle.handle_id);
        }

        new_handle
    }

    /// Return a reference to the map of managed objects for this instance.
    fn managed_map(&self) -> &HashMap<Uuid, ObjectHandle> {
        self.managed.get(&self.instance_id).unwrap()
    }

    /// Return a mutable reference to the map of managed objects for this instance.
    fn managed_map_mut(&mut self) -> &mut HashMap<Uuid, ObjectHandle> {
        self.managed.get_mut(&self.instance_id).unwrap()
    }

    /// Return whether there is a managed object with the given `id` in the repository.
    pub fn contains_managed(&self, id: Uuid) -> bool {
        self.managed_map().contains_key(&id)
    }

    /// Add a new managed object with a given `id` to the repository and return it.
    ///
    /// If another managed object with the same `id` already exists, it is replaced.
    pub fn add_managed(&mut self, id: Uuid) -> Object {
        let handle = self.add_unmanaged();
        if let Some(old_handle) = self.managed_map_mut().insert(id, handle) {
            self.remove_unmanaged(&old_handle);
        }
        let handle = self
            .managed
            .get_mut(&self.instance_id)
            .unwrap()
            .get_mut(&id)
            .unwrap();
        Object::new(&mut self.state, handle)
    }

    /// Remove the managed object with the given `id`.
    ///
    /// This returns `true` if the managed object was removed and `false` if it didn't exist.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called.
    pub fn remove_managed(&mut self, id: Uuid) -> bool {
        let handle = match self.managed_map_mut().remove(&id) {
            Some(handle) => handle,
            None => return false,
        };
        self.remove_unmanaged(&handle);
        true
    }

    /// Get a `ReadOnlyObject` for reading the managed object associated with `id`.
    ///
    /// This returns `None` if there is no managed object associated with `id`.
    ///
    /// The returned object provides read-only access to the data. To get read-write access, use
    /// `managed_object_mut`.
    pub fn managed_object(&self, id: Uuid) -> Option<ReadOnlyObject> {
        let handle = self.managed_map().get(&id)?;
        Some(ReadOnlyObject::new(&self.state, handle))
    }

    /// Get an `Object` for reading and writing the managed object associated with `id`.
    ///
    /// This returns `None` if there is no managed object associated with `id`.
    ///
    /// The returned object provides read-write access to the data. To get read-only access, use
    /// `managed_object`.
    pub fn managed_object_mut(&mut self, id: Uuid) -> Option<Object> {
        let handle = self
            .managed
            .get_mut(&self.instance_id)
            .unwrap()
            .get_mut(&id)
            .unwrap();
        Some(Object::new(&mut self.state, handle))
    }

    /// Add a new managed object at `dest` which references the same data as `source`.
    ///
    /// This returns `true` if the object was cloned or `false` if the `source` object doesn't
    /// exist.
    pub fn copy_managed(&mut self, source: Uuid, dest: Uuid) -> bool {
        // Temporarily remove this from the map to appease the borrow checker since we can't clone
        // it.
        let old_handle = match self.managed_map_mut().remove(&source) {
            Some(handle) => handle,
            None => return false,
        };
        let new_handle = self.copy_unmanaged(&old_handle);
        self.managed_map_mut().insert(source, old_handle);
        self.managed_map_mut().insert(dest, new_handle);
        true
    }

    /// Return an iterator of the IDs of managed objects stored in the repository.
    pub fn list_managed<'a>(&'a self) -> impl Iterator<Item = Uuid> + 'a {
        self.managed[&self.instance_id].keys().copied()
    }

    /// Return this repository's instance ID.
    pub fn instance(&self) -> Uuid {
        self.instance_id
    }

    /// Set this repository's instance `id`.
    pub fn set_instance(&mut self, id: Uuid) {
        // If the given instance ID is not in the managed object map, add it.
        self.managed.entry(id).or_insert_with(HashMap::new);
        self.instance_id = id;
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

    /// Commit changes which have been made to the repository.
    ///
    /// No changes are saved persistently until this method is called. Committing a repository is an
    /// atomic and consistent operation; changes cannot be partially committed and interrupting a
    /// commit will never leave the repository in an inconsistent state.
    ///
    /// If this method returns `Ok`, changes have been committed. If this method returns `Err`,
    /// changes have not been committed.
    ///
    /// This method will attempt to call `clean` once changes are committed, but will ignore any
    /// errors returned by it. This means that this method will always return `Ok` if changes have
    /// been committed, even if cleaning the repository afterwards fails. If you need to ensure that
    /// cleaning the repository succeeded or handle errors returned by that method, you should call
    /// `clean` manually.
    ///
    /// This method commits changes from all instances of the repository.
    ///
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn commit(&mut self) -> crate::Result<()> {
        // Temporarily replace the values in the repository which need to be serialized so we can
        // put them into the `Header`. This avoids the need to clone them. We'll put them back
        // later.
        let header = Header {
            chunks: mem::replace(&mut self.state.chunks, HashMap::new()),
            packs: mem::replace(&mut self.state.packs, HashMap::new()),
            managed: mem::replace(&mut self.managed, HashMap::new()),
            handle_table: mem::replace(&mut self.handle_table, IdTable::new()),
        };

        // Serialize and encode the header so we can write it to the data store.
        let serialized_header =
            to_vec(&header).expect("Could not serialize the repository header.");
        let encoded_header = self.state.encode_data(serialized_header.as_slice())?;

        // Unpack the values from the `Header` and put them back where they originally were.
        let Header {
            chunks,
            packs,
            managed,
            handle_table,
        } = header;
        self.state.chunks = chunks;
        self.state.packs = packs;
        self.managed = managed;
        self.handle_table = handle_table;

        // Write the serialized and encoded header to the data store.
        let header_id = Uuid::new_v4();
        self.state
            .store
            .lock()
            .unwrap()
            .write_block(header_id, &encoded_header)
            .map_err(|error| crate::Error::Store(error))?;
        self.state.metadata.header_id = header_id;

        // Write the repository metadata, atomically completing the commit.
        let serialized_metadata =
            to_vec(&self.state.metadata).expect("Could not serialize repository metadata.");
        self.state
            .store
            .lock()
            .unwrap()
            .write_block(METADATA_BLOCK_ID, &serialized_metadata)
            .map_err(|error| crate::Error::Store(error))?;

        // Ignore errors cleaning the repository.
        self.clean().ok();

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
    /// # Errors
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn rollback(&mut self) -> crate::Result<()> {
        // Read the previous header from the data store.
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

        let Header {
            chunks: old_chunks,
            packs: old_packs,
            managed: old_managed,
            handle_table: old_handle_table,
        } = header;

        // Replace the current header values with the ones from the previous commit.
        let current_chunks = mem::replace(&mut self.state.chunks, old_chunks);
        let current_packs = mem::replace(&mut self.state.packs, old_packs);
        let current_managed = mem::replace(&mut self.managed, old_managed);
        let current_handle_table = mem::replace(&mut self.handle_table, old_handle_table);

        if let Err(error) = self.clean() {
            // If cleaning the repository did not succeed, undo the rollback.
            self.state.chunks = current_chunks;
            self.state.packs = current_packs;
            self.managed = current_managed;
            self.handle_table = current_handle_table;
            return Err(error);
        }

        Ok(())
    }

    /// Clean up the repository to reclaim space in the backing data store.
    ///
    /// When data in a repository is deleted, the space is not reclaimed in the backing data store
    /// until those changes are committed and this method is called. This method is automatically
    /// called when changes are committed, but any errors returned are ignored. You only need to
    /// call this method manually if you need to handle these errors and ensure that all space in
    /// the backing data store has been reclaimed.
    ///
    /// # Errors
    /// - `Error::Store`: An error occurred with the data store.
    pub fn clean(&self) -> crate::Result<()> {
        let data_blocks = self.list_data_blocks()?;

        let referenced_chunks = self
            .state
            .chunks
            .values()
            .map(|info| info.block_id)
            .collect::<HashSet<_>>();

        let mut store = self.state.store.lock().unwrap();
        for stored_chunk in data_blocks {
            if !referenced_chunks.contains(&stored_chunk) {
                store
                    .remove_block(stored_chunk)
                    .map_err(|error| crate::Error::Store(error))?;
            }
        }

        Ok(())
    }

    /// Verify the integrity of all the data in every instance the repository.
    ///
    /// This verifies the integrity of all the data in the repository and returns an
    /// `IntegrityReport` containing the results.
    ///
    /// If you just need to verify the integrity of one object, `Object::verify` is faster. If you
    /// need to verify the integrity of all the data in the repository, however, this can be more
    /// efficient.
    ///
    /// # Errors
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&mut self) -> crate::Result<IntegrityReport> {
        let mut report = IntegrityReport {
            corrupt_chunks: HashSet::new(),
            corrupt_managed: HashMap::new(),
        };

        let expected_chunks = self.state.chunks.keys().copied().collect::<Vec<_>>();

        // Get the set of hashes of chunks which are corrupt.
        let mut chunk_reader = StoreReader::new(&self.state);
        for chunk in expected_chunks {
            match chunk_reader.read_chunk(chunk) {
                Ok(data) => {
                    if data.len() != chunk.size || chunk_hash(&data) != chunk.hash {
                        report.corrupt_chunks.insert(chunk.hash);
                    }
                }
                Err(crate::Error::InvalidData) => {
                    // Ciphertext verification failed. No need to check the hash.
                    report.corrupt_chunks.insert(chunk.hash);
                }
                Err(error) => return Err(error),
            };
        }

        // If there are no corrupt chunks, there are no corrupt objects.
        if report.corrupt_chunks.is_empty() {
            return Ok(report);
        }

        for (instance_id, managed) in &self.managed {
            for (object_id, handle) in managed {
                for chunk in &handle.chunks {
                    // If any one of the object's chunks is corrupt, the object is corrupt.
                    if report.corrupt_chunks.contains(&chunk.hash) {
                        report
                            .corrupt_managed
                            .entry(*instance_id)
                            .or_default()
                            .insert(*object_id);
                        break;
                    }
                }
            }
        }

        Ok(report)
    }

    /// Change the password for this repository.
    ///
    /// This replaces the existing password with `new_password`. Changing the password does not
    /// require re-encrypting any data. The change does not take effect until `commit` is called.
    /// If encryption is disabled, this method does nothing.
    pub fn change_password(&mut self, new_password: &[u8]) {
        let salt = KeySalt::generate();
        let user_key = EncryptionKey::derive(
            new_password,
            &salt,
            self.state.metadata.encryption.key_size(),
            self.state.metadata.memory_limit,
            self.state.metadata.operations_limit,
        );

        let encrypted_master_key = self
            .state
            .metadata
            .encryption
            .encrypt(self.state.master_key.expose_secret(), &user_key);

        self.state.metadata.salt = salt;
        self.state.metadata.master_key = encrypted_master_key;
    }

    /// Return information about the repository.
    pub fn info(&self) -> RepoInfo {
        self.state.metadata.to_info()
    }

    /// Return information about the repository in `store` without opening it.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no repository in the given `store`.
    /// - `Error::Corrupt`: The repository is corrupt. This is most likely unrecoverable.
    /// - `Error::Store`: An error occurred with the data store.
    pub fn peek_info(store: &mut impl DataStore) -> crate::Result<RepoInfo> {
        // Read and deserialize the metadata.
        let serialized_metadata = match store
            .read_block(METADATA_BLOCK_ID)
            .map_err(|error| crate::Error::Store(error))?
        {
            Some(data) => data,
            None => return Err(crate::Error::NotFound),
        };
        let metadata: RepoMetadata =
            from_read(serialized_metadata.as_slice()).map_err(|_| crate::Error::Corrupt)?;

        Ok(metadata.to_info())
    }

    /// Consume this repository and return the wrapped `DataStore`.
    ///
    /// This rolls back any uncommitted changes.
    pub fn into_store(self) -> Box<dyn DataStore> {
        self.state.store.into_inner().unwrap()
    }
}
