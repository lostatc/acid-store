/*
 * Copyright 2019 Garrett Powell
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

use std::cmp::min;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::iter;
use std::path::Path;

use cdchunking::Chunker;
use cdchunking::ZPAQ;
use fs2::FileExt;
use iter_read::IterRead;
use num_integer::div_ceil;
use num_integer::div_floor;
use rmp_serde::{from_read, to_vec};
use serde::de::DeserializeOwned;
use serde::Serialize;
use uuid::Uuid;

use super::block::{Chunk, Extent, pad_to_block_size, SuperBlock};
use super::config::ArchiveConfig;
use super::encryption::{Encryption, Key};
use super::header::Header;
use super::object::{chunk_hash, ChunkHash, Object};

/// A persistent object store.
///
/// An `ObjectArchive` is a binary file format for efficiently storing binary data. An object
/// archive maps keys of type `K` to binary blobs called objects.
///
/// To store data in an object archive, you first `write` or `serialize` data to the archive and
/// receive an `Object` which represents that data. Then, you can `insert` that object into the
/// archive with a key.
///
/// Data in an object archive is transparently deduplicated using content-defined block-level
/// deduplication. The data and metadata in the archive can optionally be compressed and encrypted.
///
/// Object archives use file locking to prevent multiple processes from opening the same archive at
/// the same time. This does not protect against the same archive being opened multiple times
/// within the same process, however. Doing so breaks ACID guarantees.
///
/// Changes made to an `ObjectArchive` are not persisted to disk until `commit` is called.
pub struct ObjectArchive<K>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
{
    /// The superblock for this archive.
    superblock: SuperBlock,

    /// The header as of the last time changes were committed.
    old_header: Header<K>,

    /// The current header of the archive.
    header: Header<K>,

    /// The file handle for the archive.
    archive_file: File,

    /// The encryption key for the repository.
    encryption_key: Key,
}

impl<K> ObjectArchive<K>
where
    K: Eq + Hash + Clone + Serialize + DeserializeOwned,
{
    /// Create a new archive at the given `path` with the given `config`.
    ///
    /// If encryption is enabled, an `encryption_key` must be provided. Otherwise, this argument
    /// can be `None`.
    ///
    /// # Errors
    /// - `ErrorKind::PermissionDenied`: The user lacks permission to create the archive file.
    /// - `ErrorKind::AlreadyExists`: A file already exists at `path`.
    /// - `ErrorKind::InvalidInput`: A key was required but not provided.
    /// - `ErrorKind::WouldBlock`: The archive is in use by another process.
    pub fn create(
        path: &Path,
        config: ArchiveConfig,
        encryption_key: Option<Key>,
    ) -> io::Result<Self> {
        let mut archive_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;

        archive_file.try_lock_exclusive()?;

        // Create a new superblock without a reference to a header.
        let superblock = SuperBlock {
            id: Uuid::new_v4(),
            block_size: config.block_size,
            chunker_bits: config.chunker_bits,
            compression: config.compression,
            encryption: config.encryption,
            header: Extent {
                index: 0,
                blocks: 0,
            },
            header_size: 0,
        };

        // Return an error if a key was required but not provided.
        if encryption_key == None && superblock.encryption != Encryption::None {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "A key was required but not provided",
            ));
        }

        // Create an empty key if a key is not needed.
        let encryption_key = encryption_key.unwrap_or(Key::new(vec![0u8; 0]));

        // Write the superblock to disk.
        superblock.write(&mut archive_file)?;

        // Create a new archive with an empty header.
        let mut archive = ObjectArchive {
            superblock,
            old_header: Default::default(),
            header: Default::default(),
            archive_file,
            encryption_key,
        };

        // Write the header to disk.
        archive.commit()?;

        Ok(archive)
    }

    /// Opens the archive at the given `path`.
    ///
    /// If encryption is enabled, an `encryption_key` must be provided. Otherwise, this argument can
    /// be `None`.
    ///
    /// # Errors
    /// - `ErrorKind::NotFound`: The archive file does not exist.
    /// - `ErrorKind::PermissionDenied`: The user lacks permission to open the archive file.
    /// - `ErrorKind::InvalidInput`: A key was required but not provided.
    /// - `ErrorKind::InvalidData`: The header is corrupt.
    /// - `ErrorKind::InvalidData`: The wrong encryption key was provided.
    /// - `ErrorKind::WouldBlock`: The archive is in use by another process.
    pub fn open(path: &Path, encryption_key: Option<Key>) -> io::Result<Self> {
        let mut archive_file = OpenOptions::new().read(true).write(true).open(path)?;

        archive_file.try_lock_exclusive()?;

        // Read the superblock.
        let superblock = SuperBlock::read(&mut archive_file)?;

        // Return an error if a key was required but not provided.
        if encryption_key == None && superblock.encryption != Encryption::None {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "A key was required but not provided",
            ));
        }

        // Create an empty key if a key is not needed.
        let encryption_key = encryption_key.unwrap_or(Key::new(vec![0u8; 0]));

        // Create the new archive without its header.
        let mut archive = ObjectArchive {
            superblock,
            old_header: Default::default(),
            header: Default::default(),
            archive_file,
            encryption_key,
        };

        // Decode the header.
        let mut encoded_header = archive.read_extent(archive.superblock.header)?;
        encoded_header.truncate(archive.superblock.header_size as usize);
        let decoded_header = archive.decode_data(encoded_header.as_slice())?;

        // Deserialize the header.
        let header: Header<K> = from_read(decoded_header.as_slice())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "The header is corrupt."))?;

        // Add the header to the archive.
        archive.old_header = header.clone();
        archive.header = header;

        Ok(archive)
    }

    /// Adds an `object` with the given `key` to the archive and returns it.
    ///
    /// If an object with the given `key` already existed in the archive, it is replaced and the old
    /// object is returned. Otherwise, `None` is returned.
    pub fn insert(&mut self, key: K, object: Object) -> Option<Object> {
        self.header.objects.insert(key, object)
    }

    /// Removes and returns the object with the given `key` from the archive.
    ///
    /// This returns `None` if there is no object with the given `key`.
    ///
    /// The space used by the given object isn't freed and made available for new objects until
    /// `commit` is called. The size of the archive file will not shrink unless `repack` is called.
    pub fn remove(&mut self, key: &K) -> Option<Object> {
        let result = self.header.objects.remove(key);
        self.header.clean_chunks();
        result
    }

    /// Returns a reference to the object with the given `key`, or `None` if it doesn't exist.
    pub fn get(&self, key: &K) -> Option<&Object> {
        self.header.objects.get(key)
    }

    /// Returns an iterator over all the keys in this archive.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.header.objects.keys()
    }

    /// Returns an iterator over all the keys and objects in this archive.
    pub fn objects(&self) -> impl Iterator<Item = (&K, &Object)> {
        self.header.objects.iter()
    }

    /// Returns the data in the given `extent`.
    fn read_extent(&self, extent: Extent) -> io::Result<Vec<u8>> {
        let mut archive_file = &self.archive_file;

        archive_file.seek(SeekFrom::Start(extent.start(self.superblock.block_size)))?;

        let mut buffer = Vec::with_capacity(extent.length(self.superblock.block_size) as usize);

        self.archive_file
            .try_clone()?
            .take(extent.length(self.superblock.block_size))
            .read_to_end(&mut buffer)?;

        Ok(buffer)
    }

    /// Writes the given `data` to the given `extent` and returns the number of bytes written.
    fn write_extent(&mut self, extent: Extent, data: &[u8]) -> io::Result<usize> {
        self.archive_file
            .seek(SeekFrom::Start(extent.start(self.superblock.block_size)))?;

        let bytes_written = min(
            data.len(),
            extent.length(self.superblock.block_size) as usize,
        );

        self.archive_file.write_all(&data[..bytes_written])?;

        Ok(bytes_written)
    }

    /// Returns a list of extents which are unused and can be overwritten.
    ///
    /// The returned extents are sorted by their location in the file.
    fn unused_extents(&mut self) -> io::Result<Vec<Extent>> {
        // Get all extents which are currently part of a chunk.
        let mut used_extents = self.header.extents();

        // Include all extents which were in use the last time changes were committed.
        // We must not overwrite this data in case changes made since then aren't committed.
        used_extents.extend(self.old_header.extents());

        // Include the extent storing the header.
        used_extents.push(self.superblock.header);

        // Sort extents by their location in the file.
        used_extents.sort_by_key(|extent| extent.index);

        // Remove duplicate extents.
        used_extents.dedup();

        // Get the extents which are unused.
        let initial_extent = Extent {
            index: 0,
            blocks: 0,
        };
        let unused_extents = iter::once(initial_extent)
            .chain(used_extents)
            .collect::<Vec<_>>()
            .windows(2)
            .filter_map(|pair| pair[0].between(pair[1]))
            .collect::<Vec<_>>();

        Ok(unused_extents)
    }

    /// Allocate a new extent of at least `size` bytes at the end of the archive.
    ///
    /// This pads the file so that the new extent is aligned with the block size.
    fn new_extent(&mut self, size: u64) -> io::Result<Extent> {
        let offset = pad_to_block_size(&mut self.archive_file, self.superblock.block_size)?;
        let index = div_floor(offset, self.superblock.block_size as u64);
        let blocks = div_ceil(size, self.superblock.block_size as u64);
        Ok(Extent { index, blocks })
    }

    /// Truncate the given extent so it is just large enough to hold `size` bytes.
    fn truncate_extent(&self, extent: Extent, size: u64) -> Extent {
        let blocks = min(
            extent.blocks,
            div_ceil(size, self.superblock.block_size as u64),
        );
        Extent {
            index: extent.index,
            blocks,
        }
    }

    /// Allocate a contiguous extent of at least `size` bytes.
    fn allocate_contiguous_extent(&mut self, size: u64) -> io::Result<Extent> {
        // Look for an unused extent that is large enough.
        let unused_extents = self.unused_extents()?;
        let allocated_extent = unused_extents
            .iter()
            .find(|extent| extent.length(self.superblock.block_size) >= size);

        // If there is no unused extent large enough, allocate a new extent at the end of the file.
        let allocated_extent = match allocated_extent {
            Some(extent) => self.truncate_extent(*extent, size),
            None => self.new_extent(size)?,
        };

        Ok(allocated_extent)
    }

    /// Allocate a list of extents with a combined size of at least `size` bytes.
    fn allocate_extents(&mut self, size: u64) -> io::Result<Vec<Extent>> {
        let mut allocated_extents = Vec::new();
        let mut bytes_remaining = size;

        // Try to fill all unused extents first.
        for extent in self.unused_extents()? {
            let new_extent = self.truncate_extent(extent, bytes_remaining);
            allocated_extents.push(new_extent);
            bytes_remaining -= min(
                bytes_remaining,
                new_extent.length(self.superblock.block_size),
            );
        }

        // If there's still data left, allocate an extent at the end of the file to store the rest.
        if bytes_remaining > 0 {
            allocated_extents.push(self.new_extent(bytes_remaining)?);
        }

        Ok(allocated_extents)
    }

    /// Compresses and encrypts the given `data` and returns it.
    fn encode_data(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let compressed_data = self.superblock.compression.compress(data)?;

        Ok(self
            .superblock
            .encryption
            .encrypt(compressed_data.as_slice(), &self.encryption_key))
    }

    /// Decrypts and decompresses the given `data` and returns it.
    fn decode_data(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let decrypted_data = self
            .superblock
            .encryption
            .decrypt(data, &self.encryption_key)?;

        Ok(self
            .superblock
            .compression
            .decompress(decrypted_data.as_slice())?)
    }

    /// Writes the given `data` as a new chunk and returns its checksum.
    ///
    /// If a chunk with the given `data` already exists, its checksum is returned and no new data is
    /// written.
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<ChunkHash> {
        // Get a checksum of the unencoded data.
        let checksum = chunk_hash(data);

        // Check if the chunk already exists.
        if self.header.chunks.contains_key(&checksum) {
            return Ok(checksum);
        }

        // Encode the data.
        let encoded_data = self.encode_data(data)?;

        // Get the list of extents which will hold the data.
        let extents = self.allocate_extents(encoded_data.len() as u64)?;

        // Write the encoded data to the extents.
        let mut bytes_written = 0;
        for extent in &extents {
            bytes_written += self.write_extent(*extent, &encoded_data[bytes_written..])?;
        }

        // Add this chunk to the header.
        self.header.chunks.insert(
            checksum,
            Chunk {
                size: encoded_data.len() as u64,
                extents,
            },
        );

        Ok(checksum)
    }

    /// Returns the bytes of the chunk with the given checksum, or `None` if there is none.
    fn read_chunk(&self, checksum: &ChunkHash) -> io::Result<Vec<u8>> {
        // Get the chunk with the given checksum.
        let chunk = self.header.chunks[checksum].clone();

        // Allocate a buffer big enough to hold the chunk's data and any extra data left at the end
        // of the last extent.
        let mut chunk_data =
            Vec::with_capacity(chunk.size as usize + self.superblock.block_size as usize);

        // Read the contents of each extent in the chunk into the buffer.
        for extent in chunk.extents {
            chunk_data.append(&mut self.read_extent(extent)?);
        }

        // Drop bytes which aren't part of this chunk.
        chunk_data.truncate(chunk.size as usize);

        // Decode the contents of the chunk.
        let decoded_data = self.decode_data(&chunk_data)?;

        Ok(decoded_data)
    }

    /// Writes the given `data` to the archive and returns a new object.
    pub fn write(&mut self, data: impl Read) -> io::Result<Object> {
        let chunker = Chunker::new(ZPAQ::new(self.superblock.chunker_bits as usize));

        let mut checksums = Vec::new();
        let mut size = 0u64;

        // Split the data into content-defined chunks and write those chunks to the archive.
        for chunk_result in chunker.whole_chunks(data) {
            let chunk = chunk_result?;
            size += chunk.len() as u64;
            checksums.push(self.write_chunk(&chunk)?);
        }

        Ok(Object {
            size,
            chunks: checksums,
        })
    }

    /// Serializes the given `value`, writes it to the archive, and returns a new object.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidInput`: The given `value` could not be serialized.
    pub fn serialize(&mut self, value: &impl Serialize) -> io::Result<Object> {
        let serialized_value =
            to_vec(&value).map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        self.write(serialized_value.as_slice())
    }

    /// Returns a reader for reading the data associated with `object` from the archive.
    pub fn read<'a>(&'a self, object: &'a Object) -> impl Read + 'a {
        let chunks = object
            .chunks
            .iter()
            .map(move |checksum| self.read_chunk(checksum));

        IterRead::new(chunks)
    }

    /// Returns a buffer containing the data associated with `object`.
    pub fn read_all(&self, object: &Object) -> io::Result<Vec<u8>> {
        let mut data = Vec::with_capacity(object.size() as usize);
        self.read(object).read_to_end(&mut data)?;
        Ok(data)
    }

    /// Deserializes and returns the data associated with `object`.
    ///
    /// # Errors
    /// - `ErrorKind::InvalidData`: The data could not be deserialized as a value of type `T`.
    pub fn deserialize<T: DeserializeOwned>(&self, object: &Object) -> io::Result<T> {
        let serialized_value = self.read_all(object)?;
        from_read(serialized_value.as_slice())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Commit changes which have been made to the archive.
    ///
    /// No changes are saved persistently until this method is called. Committing an archive is an
    /// atomic and consistent operation; changes cannot be partially committed and interrupting a
    /// commit will never leave the archive in an inconsistent state.
    pub fn commit(&mut self) -> io::Result<()> {
        // Serialize and encode the header.
        let serialized_header = to_vec(&self.header).expect("Could not serialize header.");
        let encoded_header = self.encode_data(&serialized_header)?;

        // The entire header must be stored in a single extent. Allocate space for it.
        let header_extent = self.allocate_contiguous_extent(encoded_header.len() as u64)?;

        // Write the header to the chosen extent.
        self.write_extent(header_extent, &encoded_header)?;

        // Update the superblock to point to the new header.
        self.superblock.header = header_extent;
        self.superblock.header_size = encoded_header.len() as u32;

        // Write the new superblock, atomically completing the commit.
        self.superblock.write(&mut self.archive_file)?;

        // Now that changes have been committed, the `old_header` field should be updated.
        self.old_header = self.header.clone();

        Ok(())
    }

    /// Copy the contents of this archive to a new archive file at `destination`.
    ///
    /// Archives can reuse space left over from deleted objects, but they can not deallocate space
    /// which has been allocated. This means that archive files can grow in size, but never shrink.
    ///
    /// This method copies the data in this archive to a new file, allocating the minimum amount of
    /// space necessary to store it. This can result in a significantly smaller archive if a lot of
    /// data has been removed from this archive and not replaced with new data.
    ///
    /// Like file systems, archive files can become fragmented over time. The new archive will be
    /// defragmented.
    ///
    /// This method returns the new archive. Both this archive and the returned archive will be
    /// usable after this method returns. Uncommitted changes will not be copied to the new archive.
    ///
    /// # Errors
    /// - `ErrorKind::PermissionDenied`: The user lacks permission to create the archive file.
    /// - `ErrorKind::AlreadyExists`: A file already exists at `destination`.
    pub fn repack(&self, destination: &Path) -> io::Result<ObjectArchive<K>> {
        let mut dest_archive = Self::create(
            destination,
            self.superblock.to_config(),
            Some(self.encryption_key.clone()),
        )?;

        for (key, object) in self.header.objects.iter() {
            let dest_object = dest_archive.write(self.read(&object))?;
            dest_archive.insert(key.clone(), dest_object);
        }

        dest_archive.commit()?;

        Ok(dest_archive)
    }

    /// Verify the integrity of the data associated with `object`.
    ///
    /// This returns `true` if the object is valid and `false` if it is corrupt.
    pub fn verify_object(&self, object: &Object) -> io::Result<bool> {
        for expected_checksum in &object.chunks {
            let data = self.read_chunk(expected_checksum)?;
            let actual_checksum = chunk_hash(&data);
            if *expected_checksum != actual_checksum {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Verify the integrity of all the data in the archive.
    ///
    /// This returns `true` if the archive is valid and `false` if some data in it is corrupt.
    pub fn verify_archive(&self) -> io::Result<bool> {
        for expected_checksum in self.header.chunks.keys() {
            let data = self.read_chunk(expected_checksum)?;
            let actual_checksum = chunk_hash(&data);
            if *expected_checksum != actual_checksum {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Return the UUID of the archive.
    ///
    /// Every archive has a UUID associated with it.
    pub fn uuid(&self) -> Uuid {
        self.superblock.id
    }

    /// Return the UUID of the archive at `path` without opening it.
    ///
    /// Every archive has a UUID associated with it. Reading the UUID does not require decrypting
    /// the archive.
    ///
    /// # Errors
    /// - `ErrorKind::NotFound`: The archive file does not exist.
    /// - `ErrorKind::PermissionDenied`: The user lacks permission to open the archive file.
    /// - `ErrorKind::WouldBlock`: The archive is in use by another process.
    pub fn peek_uuid(path: &Path) -> io::Result<Uuid> {
        // We must open the file for writing because reading the superblock may involve repairing a
        // corrupt superblock.
        let mut archive_file = OpenOptions::new().read(true).write(true).open(path)?;
        archive_file.try_lock_exclusive()?;
        let superblock = SuperBlock::read(&mut archive_file)?;
        Ok(superblock.id)
    }
}
