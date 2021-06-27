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

use std::convert::TryFrom;
use std::fmt::Debug;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, RwLock, Weak};

use rmp_serde::{from_read, to_vec};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::handle::{ContentId, ObjectHandle};
use super::object_io::{ObjectInfo, ObjectReader, ObjectWriter};
use super::state::ObjectState;
use super::state::RepoState;

/// A read-write view of data in a repository.
///
/// An `Object` is a view of data in a repository. It implements `Read`, `Write`, and `Seek` for
/// reading data from the repository and writing data to the repository.
///
/// Writing to an `Object` is transactional—writing to an object via `Write` automatically begins a
/// transaction, and calling [`commit`] completes the transaction and commits changes to the
/// repository. No data is persisted to the repository until the transaction is committed.
/// Changes made to this object are not visible to other `Object` or `ReadOnlyObject` instances
/// until the transaction is complete. When an `Object` is dropped, any bytes written since the last
/// commit are discarded.
///
/// Attempting to read or seek on an `Object` with uncommitted changes will always return
/// [`Error::TransactionInProgress`]. Attempting to write to an `Object` if another `Object`
/// instance already has a transaction in progress will also return
/// [`Error::TransactionInProgress`].
///
/// Because `Object` internally buffers data when reading, there's no need to use a buffered reader
/// like `BufReader`.
///
/// If encryption is enabled for the repository, data integrity is automatically verified as it is
/// read and methods will return an `Err` if corrupt data is found. The [`verify`] method can be
/// used to check the integrity of all the data in the object whether encryption is enabled or not.
///
/// The methods of `Read`, `Write`, and `Seek` return `io::Result`, but the returned `io::Error` can
/// be converted `Into` an `acid_store::Error` to be consistent with the rest of the library.
///
/// [`commit`]: crate::repo::Object::commit
/// [`Commit::clean`]: crate::repo::Commit::clean
/// [`Error::TransactionInProgress`]: crate::Error::TransactionInProgress
/// [`verify`]: crate::repo::Object::verify
#[derive(Debug)]
pub struct Object {
    /// The state for the object repository.
    repo_state: Weak<RwLock<RepoState>>,

    /// The object handle which stores the hashes of the chunks which make up the object.
    handle: Weak<RwLock<ObjectHandle>>,

    /// The state for the object itself.
    object_state: ObjectState,
}

impl Object {
    pub(super) fn new(
        repo_state: &Arc<RwLock<RepoState>>,
        handle: &Arc<RwLock<ObjectHandle>>,
    ) -> Self {
        let metadata = &repo_state.read().unwrap().metadata;
        let object_state = ObjectState::new(metadata.config.chunking.to_chunker());
        Self {
            repo_state: Arc::downgrade(repo_state),
            handle: Arc::downgrade(handle),
            object_state,
        }
    }

    fn object_info(&self) -> crate::Result<ObjectInfo> {
        ObjectInfo::new(&self.repo_state, &self.object_state, &self.handle)
    }

    fn object_reader(&mut self) -> crate::Result<ObjectReader> {
        ObjectReader::new(&self.repo_state, &mut self.object_state, &self.handle)
    }

    fn object_writer(&mut self) -> crate::Result<ObjectWriter> {
        ObjectWriter::new(&self.repo_state, &mut self.object_state, &self.handle)
    }

    /// Return the size of the object in bytes.
    ///
    /// # Errors
    /// - `Error::TransactionInProgress`: A transaction is currently in progress for this object.
    /// - `Error::InvalidObject`: The repository associated with this object was dropped or the
    /// object was removed.
    pub fn size(&self) -> crate::Result<u64> {
        self.object_info()?.size()
    }

    /// Return a `ContentId` representing the contents of the object.
    ///
    /// Calculating a content ID is cheap. This method does not read any data from the data store.
    ///
    /// The returned `ContentId` represents the contents of the object at the time this method was
    /// called. It is not updated when the object is modified.
    ///
    /// # Errors
    /// - `Error::TransactionInProgress`: A transaction is currently in progress for this object.
    /// - `Error::InvalidObject`: The repository associated with this object was dropped or the
    /// object was removed.
    pub fn content_id(&self) -> crate::Result<ContentId> {
        self.object_info()?.content_id()
    }

    /// Verify the integrity of the data in this object.
    ///
    /// This returns `true` if the object is valid and `false` if it is corrupt.
    ///
    /// # Errors
    /// - `Error::TransactionInProgress`: A transaction is currently in progress for this object.
    /// - `Error::InvalidObject`: The repository associated with this object was dropped or the
    /// object was removed.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn verify(&mut self) -> crate::Result<bool> {
        self.object_reader()?.verify()
    }

    /// Truncate the object to the given `length`.
    ///
    /// If the given `length` is greater than or equal to the current size of the object, this does
    /// nothing. If the seek position is past the point which the object is truncated to, it is
    /// moved to the new end of the object.
    ///
    /// This method starts a new transaction and commits the transaction before it returns.
    ///
    /// # Errors
    /// - `Error::TransactionInProgress`: A transaction is currently in progress for this object.
    /// - `Error::InvalidObject`: The repository associated with this object was dropped or the
    /// object was removed.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn truncate(&mut self, length: u64) -> crate::Result<()> {
        self.object_writer()?.truncate(length)
    }

    /// Serialize the given `value` and write it to the object.
    ///
    /// This is a convenience function that serializes the `value` using a space-efficient binary
    /// format, overwrites all the data in the object, and truncates it to the length of the
    /// serialized `value`.
    ///
    /// This method starts a new transaction and commits the transaction once it returns.
    ///
    /// # Errors
    /// - `Error::Serialize`: The given value could not be serialized.
    /// - `Error::TransactionInProgress`: A transaction is currently in progress for this object.
    /// - `Error::InvalidObject`: The repository associated with this object was dropped or the
    /// object was removed.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn serialize<T: Serialize>(&mut self, value: &T) -> crate::Result<()> {
        let serialized = to_vec(value).map_err(|_| crate::Error::Serialize)?;
        let mut writer = self.object_writer()?;
        writer.seek(SeekFrom::Start(0))?;
        writer.write_all(serialized.as_slice())?;
        writer.commit()?;
        writer.truncate(serialized.len() as u64)?;
        Ok(())
    }

    /// Deserialize a value serialized with `Object::serialize`.
    ///
    /// This is a convenience function that deserializes a value serialized to the object with
    /// `Object::serialize`
    ///
    /// # Errors
    /// - `Error::Deserialize`: The data could not be deserialized as a value of type `T`.
    /// - `Error::TransactionInProgress`: A transaction is currently in progress for this object.
    /// - `Error::InvalidObject`: The repository associated with this object was dropped or the
    /// object was removed.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn deserialize<T: DeserializeOwned>(&mut self) -> crate::Result<T> {
        let mut reader = self.object_reader()?;
        reader.seek(SeekFrom::Start(0))?;
        from_read(&reader).map_err(|_| crate::Error::Deserialize)
    }

    /// Commit changes to this object to the repository.
    ///
    /// Data written to this object via `Write` is not persisted to the repository or visible to
    /// other `Object` or `ReadOnlyObject` instances until this method is called and returns `Ok`.
    ///
    /// Calling this method does not call [`Commit::commit`]. Even if this method is called, data is
    /// not persisted to the data store until [`Commit::commit`] is called on the repository this
    /// object is associated with.
    ///
    /// # Errors
    /// - `Error::InvalidObject`: The repository associated with this object was dropped or the
    /// object was removed.
    /// - `Error::InvalidData`: Ciphertext verification failed.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    ///
    /// [`Commit::commit`]: crate::repo::Commit::commit
    pub fn commit(&mut self) -> crate::Result<()> {
        self.object_writer()?.commit()
    }
}

impl Read for Object {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.object_reader()?.read(buf)
    }
}

impl Seek for Object {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.object_reader()?.seek(pos)
    }
}

impl Write for Object {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.object_writer()?.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.object_writer()?.flush()
    }
}

/// An read-only view of data in a repository.
///
/// A `ReadOnlyObject` is a view of data in a repository. It implements `Read` and `Seek` for
/// reading data from the repository. You can think of this as a read-only counterpart to
/// [`Object`].
///
/// See [`Object`] for details.
///
/// [`Object`]: crate::repo::Object
#[derive(Debug)]
pub struct ReadOnlyObject(Object);

impl ReadOnlyObject {
    pub(super) fn new(
        repo_state: &Arc<RwLock<RepoState>>,
        handle: &Arc<RwLock<ObjectHandle>>,
    ) -> Self {
        Self(Object::new(repo_state, handle))
    }

    /// Return the size of the object in bytes.
    ///
    /// See [`Object::size`] for details.
    ///
    /// [`Object::size`]: crate::repo::Object::size
    pub fn size(&self) -> crate::Result<u64> {
        self.0.size()
    }

    /// Return a `ContentId` representing the contents of this object.
    ///
    /// See [`Object::content_id`] for details.
    ///
    /// [`Object::content_id`]: crate::repo::Object::content_id
    pub fn content_id(&self) -> crate::Result<ContentId> {
        self.0.content_id()
    }

    /// Verify the integrity of the data in this object.
    ///
    /// See [`Object::verify`] for details.
    ///
    /// [`Object::verify`]: crate::repo::Object::verify
    pub fn verify(&mut self) -> crate::Result<bool> {
        self.0.verify()
    }

    /// Deserialize a value serialized with [`Object::serialize`].
    ///
    /// See [`Object::deserialize`] for details.
    ///
    /// [`Object::serialize`]: crate::repo::Object::serialize
    /// [`Object::deserialize`]: crate::repo::Object::deserialize
    pub fn deserialize<T: DeserializeOwned>(&mut self) -> crate::Result<T> {
        self.0.deserialize()
    }
}

impl TryFrom<Object> for ReadOnlyObject {
    type Error = crate::Error;

    fn try_from(value: Object) -> Result<Self, Self::Error> {
        // We need to check if there is a transaction in progress because once this is converted to
        // a `ReadOnlyObject`, it will be impossible to finish the transaction.
        if value.object_state.transaction_lock.is_some() {
            Err(crate::Error::TransactionInProgress)
        } else {
            Ok(ReadOnlyObject(value))
        }
    }
}

impl Read for ReadOnlyObject {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Seek for ReadOnlyObject {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}
