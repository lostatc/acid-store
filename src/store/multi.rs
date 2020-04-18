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
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Mutex;

use rmp_serde::{from_read, to_vec};
use uuid::Uuid;

use lazy_static::lazy_static;

use crate::repo::Key;

use super::common::DataStore;

lazy_static! {
    /// The block ID of the block which stores the data store table.
    static ref STORE_TABLE_BLOCK_ID: Uuid =
        Uuid::parse_str("02e707aa-6fb9-4807-b75a-762fdcb72a1c").unwrap();

    /// The block ID of the block which stores the repository format version.
    static ref VERSION_BLOCK_ID: Uuid =
        Uuid::parse_str("e6a7a9b2-e7a8-40d5-8569-8c80a1954014").unwrap();

    /// The current `MultiStore` format version ID.
    ///
    /// This must be changed any time a backwards-incompatible change is made to the `MultiStore`
    /// format.
    static ref VERSION_ID: Uuid =
        Uuid::parse_str("5a163dca-152b-453f-a1ff-d58a9258bbd2").unwrap();
}

/// A logical data store which delegates to a backing data store.
///
/// This is used with `MultiStore` to allow a data store to store multiple repositories.
pub struct ProxyStore<'a, S: DataStore> {
    /// The backing data store of the proxy store.
    store: &'a Mutex<S>,

    /// The ID of the block which stores the serialized ID table.
    store_id: Uuid,

    /// A mapping of block IDs in this data store to block IDs in the backing data store.
    id_table: HashMap<Uuid, Uuid>,
}

impl<'a, S: DataStore> ProxyStore<'a, S> {
    /// Create a new `ProxyStore` with a given `id` that is backed by `store`.
    fn new(store: &'a Mutex<S>, id: Uuid) -> crate::Result<Self> {
        let mut backing_store = store.lock().unwrap();
        let id_table = match backing_store.read_block(id).map_err(anyhow::Error::from)? {
            Some(serialized_id_table) => {
                // Deserialize the existing ID table.
                from_read(serialized_id_table.as_slice()).expect("Could not deserialize ID table.")
            }
            None => {
                // Create a new ID table.
                let id_table = HashMap::new();
                let serialized_id_table = to_vec(&id_table).expect("Could not serialize ID table.");
                backing_store
                    .write_block(id, serialized_id_table.as_slice())
                    .map_err(anyhow::Error::from)?;
                id_table
            }
        };

        Ok(ProxyStore {
            store,
            store_id: id,
            id_table,
        })
    }

    /// Write the ID table to the backing data store.
    fn write_table(&self) -> Result<(), S::Error> {
        let serialized_id_table = to_vec(&self.id_table).expect("Could not serialize ID table.");
        self.store
            .lock()
            .unwrap()
            .write_block(self.store_id, serialized_id_table.as_slice())
    }
}

// Because `ProxyStore` is stateful, we have to handle the situation where the ID table and the
// backing data store go out of sync. If the thread panics at the wrong time, depending on how this
// is implemented, we will either end up with blocks in the backing data store which are not in the
// ID table or blocks in the ID table which are not in the backing data store. Of these two
// situations, the latter is cheaper to clean up because it doesn't require listing all the blocks
// in the backing data store.
impl<'a, S: DataStore> DataStore for ProxyStore<'a, S> {
    type Error = S::Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let backing_id = Uuid::new_v4();
        self.id_table.insert(id, backing_id);

        // We must write the block after we write the ID table. If we wrote the block first and the
        // thread panicked before the ID table was written, we would need to go back and clean up
        // the unreferenced block later.
        self.write_table()?;
        self.store.lock().unwrap().write_block(backing_id, data)
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let backing_id = match self.id_table.get(&id) {
            Some(backing_id) => backing_id,
            None => return Ok(None),
        };

        let result = self.store.lock().unwrap().read_block(*backing_id);

        if let Ok(None) = result {
            // The block exists in the ID table, but not in the backing data store, likely because a
            // call to `write_block` or `read_block` was interrupted. We need to remove the
            // nonexistent block from the ID table.
            self.id_table.remove(&id);
        }

        result
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let backing_id = match self.id_table.remove(&id) {
            Some(backing_id) => backing_id,
            None => return Ok(()),
        };

        // We must remove the block before we write the ID table. If we were to write the ID table
        // first and the thread panicked before the block was removed, we would need to go back and
        // clean up the unreferenced block later.
        self.store.lock().unwrap().remove_block(backing_id)?;
        self.write_table()
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        let backing_ids = self
            .store
            .lock()
            .unwrap()
            .list_blocks()?
            .into_iter()
            .collect::<HashSet<_>>();

        let mut block_ids = Vec::new();
        let mut ids_to_remove = Vec::new();

        // We need to check for block IDs which are in the ID table but not in the backing data
        // store. This can happen if a call to `write_block` or `read_block` is interrupted.
        for (block_id, backing_id) in self.id_table.iter() {
            if backing_ids.contains(backing_id) {
                block_ids.push(*block_id);
            } else {
                ids_to_remove.push(*block_id);
            }
        }

        // Remove block IDs which are in the table but not in the backing data store.
        for id in ids_to_remove {
            self.id_table.remove(&id);
        }

        Ok(block_ids)
    }
}

/// A multiplexer for storing multiple repositories in one data store.
///
/// This type allows one data store to function as multiple data stores, each of which can contain a
/// separate repository. It does this by mapping block IDs in the exposed data stores to different
/// block IDs used in the backing data store. This does cause some overhead.
///
/// Every data store in the `MultiStore` is associated with a key of type `K`. Inserting a key
/// returns a `ProxyStore`, which is a sort of logical data store which delegates to the backing
/// data store of type `S`.
pub struct MultiStore<K: Key, S: DataStore> {
    /// The wrapped data store.
    store: Mutex<S>,

    /// A map of the keys associated with data stores to their IDs.
    store_table: HashMap<K, Uuid>,
}

impl<K: Key, S: DataStore> MultiStore<K, S> {
    /// Write the serialized data store table to the backing data store.
    fn write_table(&self) -> crate::Result<()> {
        let serialized_store_table =
            to_vec(&self.store_table).expect("Could not serialize data store table.");
        self.store
            .lock()
            .unwrap()
            .write_block(*STORE_TABLE_BLOCK_ID, serialized_store_table.as_slice())
            .map_err(anyhow::Error::from)?;
        Ok(())
    }

    /// Create a new `MultiStore` which wraps the given data `store`.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: The data store is not a `MultiStore` and it is not empty.
    /// - `Error::Corrupt`: The data store is corrupt. This is most likely unrecoverable.
    /// - `Error::UnsupportedFormat`: This data store is an unsupported format. This means the data
    /// store is a `MultiStore`, but it is in a format no longer supported by the library.
    /// - `Error::Store`: An error occurred with the underlying data store.
    pub fn new(mut store: S) -> crate::Result<Self> {
        let store_table = match store
            .read_block(*VERSION_BLOCK_ID)
            .map_err(anyhow::Error::from)?
        {
            // An existing `MultiStore` is being opened.
            Some(serialized_version) => {
                // Check the version ID.
                let version = Uuid::from_slice(serialized_version.as_slice())
                    .map_err(|_| crate::Error::Corrupt)?;
                if version != *VERSION_ID {
                    return Err(crate::Error::UnsupportedFormat);
                }

                // Deserialize the store table.
                let serialized_store_table = store
                    .read_block(*STORE_TABLE_BLOCK_ID)
                    .map_err(anyhow::Error::from)?
                    .ok_or(crate::Error::Corrupt)?;
                from_read(serialized_store_table.as_slice()).map_err(|_| crate::Error::Corrupt)?
            }

            // A new `MultiStore` is being created.
            None => {
                if !store.list_blocks().map_err(anyhow::Error::from)?.is_empty() {
                    return Err(crate::Error::AlreadyExists);
                }

                // Write the version ID.
                store
                    .write_block(*VERSION_BLOCK_ID, VERSION_ID.as_bytes())
                    .map_err(anyhow::Error::from)?;

                // Write an empty store table.
                let store_table = HashMap::new();
                let serialized_store_table =
                    to_vec(&store_table).expect("Could not serialize the store table.");
                store
                    .write_block(*STORE_TABLE_BLOCK_ID, serialized_store_table.as_slice())
                    .map_err(anyhow::Error::from)?;
                store_table
            }
        };

        Ok(MultiStore {
            store: Mutex::new(store),
            store_table,
        })
    }

    /// Return whether there is a data store with the given `key`.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.store_table.contains_key(key)
    }

    /// Insert a new data store with the given `key` and return it.
    ///
    /// # Errors
    /// - `Error::AlreadyExists`: There is already a data store with the given `key`.
    /// - `Error::Store`: An error occurred with the data store.
    pub fn insert(&mut self, key: K) -> crate::Result<ProxyStore<S>> {
        if self.store_table.contains_key(&key) {
            return Err(crate::Error::AlreadyExists);
        }

        let store_id = Uuid::new_v4();
        self.store_table.insert(key, store_id);
        self.write_table()?;

        Ok(ProxyStore::new(&self.store, store_id)?)
    }

    /// Remove the data store associated with `key` and all its blocks.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no data store with the given `key`.
    /// - `Error::Store`: An error occurred with the data store.
    pub fn remove<Q>(&mut self, key: &Q) -> crate::Result<()>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut store = self.get(key)?;

        let block_ids = store.list_blocks().map_err(anyhow::Error::from)?;
        for block_id in block_ids {
            store.remove_block(block_id).map_err(anyhow::Error::from)?;
        }

        self.store_table.remove(key);
        self.write_table()?;

        Ok(())
    }

    /// Return the data store associated with `key`.
    ///
    /// # Errors
    /// - `Error::NotFound`: There is no data store with the given `key`.
    /// - `Error::Store`: An error occurred with the data store.
    pub fn get<Q>(&mut self, key: &Q) -> crate::Result<ProxyStore<S>>
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let store_id = self.store_table.get(key).ok_or(crate::Error::NotFound)?;
        ProxyStore::new(&self.store, *store_id)
    }

    /// Return an iterator over all the keys in this `MultiStore`.
    pub fn keys<'a>(&'a self) -> impl Iterator<Item = &'a K> + 'a {
        self.store_table.keys()
    }
}
