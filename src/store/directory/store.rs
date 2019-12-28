/*
 * Copyright 2019 Wren Powell
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

use std::fs::{create_dir_all, File, remove_file, rename};
use std::io::{self, Read, Write};
use std::path::PathBuf;

use uuid::Uuid;
use walkdir::WalkDir;

use crate::store::{ChunkStore, MetadataStore};

/// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "2891c3da-297e-11ea-a7c9-1b8f8be4fc9b";

/// A `DataStore` which stores data in a directory in the local file system.
pub struct DirectoryStore {
    path: PathBuf,
    chunks_directory: PathBuf,
    metadata_path: PathBuf,
    metadata_tmp_path: PathBuf,
}

impl DirectoryStore {
    /// Create a new directory store at the given `path`.
    ///
    /// # Errors
    /// - `ErrorKind::AlreadyExists`: There is already a file at the given path.
    /// - `ErrorKind::PermissionDenied`: The user lacks permissions to create the directory.
    pub fn create(path: PathBuf) -> io::Result<Self> {
        create_dir_all(path)?;
        let mut version_file = File::create(path.join("version"))?;
        version_file.write_all(CURRENT_VERSION.as_bytes());
        Self::open(path)
    }

    /// Open an existing directory store at `path`.
    ///
    /// # Errors
    /// - `ErrorKind::NotFound`: There is not a directory at `path`.
    /// - `ErrorKind::InvalidData`: The directory at `path` is not a valid directory store.
    /// - `ErrorKind::PermissionDenied`: The user lacks permissions to read the directory.
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let mut version_file = File::open(path.join("version"))?;
        let mut version_id = String::new();
        version_file.read_to_string(&mut version_id)?;

        if version_id != CURRENT_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "The directory is not a valid directory store.",
            ));
        }

        Ok(DirectoryStore {
            path,
            chunks_directory: path.join("chunks"),
            metadata_path: path.join("metadata"),
            metadata_tmp_path: path.join("~metadata"),
        })
    }

    /// Return the path of the chunk with the given `id`.
    fn chunk_path(&self, id: &Uuid) -> PathBuf {
        let hex = id.to_simple().encode_lower(&mut Uuid::encode_buffer());
        self.chunks_directory.join(&hex[..2]).join(hex)
    }
}

impl ChunkStore for DirectoryStore {
    fn write_chunk(&mut self, data: &[u8]) -> io::Result<Uuid> {
        let chunk_id = Uuid::new_v4();
        let chunk_path = self.chunk_path(&chunk_id);
        create_dir_all(chunk_path.parent().unwrap())?;
        let mut file = File::create(chunk_path)?;
        file.write_all(data)?;
        Ok(chunk_id)
    }

    fn read_chunk(&self, id: &Uuid) -> io::Result<Vec<u8>> {
        let chunk_path = self.chunk_path(id);

        if chunk_path.exists() {
            let mut file = File::open(chunk_path)?;
            let mut buffer = Vec::with_capacity(file.metadata()?.len() as usize);
            file.read_to_end(&mut buffer)?;
            Ok(buffer)
        } else {
            panic!("There is no chunk with the given ID.")
        }
    }

    fn remove_chunk(&mut self, id: &Uuid) -> io::Result<()> {
        remove_file(self.chunk_path(id))
    }

    fn list_chunks(&self) -> io::Result<Box<dyn Iterator<Item=io::Result<Uuid>>>> {
        Ok(Box::new(
            WalkDir::new(self.path)
                .min_depth(2)
                .into_iter()
                .map(|result| match result {
                    Ok(entry) => Ok(Uuid::parse_str(
                        entry
                            .file_name()
                            .to_str()
                            .expect("Chunk file name is invalid."),
                    )
                        .expect("Chunk file name is invalid.")),
                    Err(error) => Err(io::Error::from(error)),
                }),
        ))
    }
}

impl MetadataStore for DirectoryStore {
    fn write_metadata(&mut self, metadata: &[u8]) -> io::Result<()> {
        let mut file = File::create(self.metadata_tmp_path)?;
        file.write_all(&metadata)?;
        rename(&self.metadata_tmp_path, &self.metadata_path)
    }

    fn read_metadata(&self) -> io::Result<Vec<u8>> {
        let mut file = File::open(self.metadata_path)?;
        let mut buffer = Vec::with_capacity(file.metadata()?.len() as usize);
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }
}
