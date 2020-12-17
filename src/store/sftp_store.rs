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

#![cfg(feature = "store-sftp")]

use std::fmt::{self, Debug, Formatter};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};

use anyhow::anyhow;
use ssh2::{self, RenameFlags, Session, Sftp};
use uuid::Uuid;

use super::data_store::DataStore;
use super::open_store::OpenStore;

// A UUID which acts as the version ID of the directory store format.
const CURRENT_VERSION: &str = "fc299876-c5ff-11ea-ada1-8b0ec1509cde";

// The names of files in the data store.
const BLOCKS_DIRECTORY: &str = "blocks";
const STAGING_DIRECTORY: &str = "stage";
const VERSION_FILE: &str = "version";

/// The authentication for an SSH connection.
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-sftp")))]
pub enum SftpAuth {
    /// Authenticate with a password.
    Password {
        /// The username to authenticate with.
        username: String,

        /// The password to authenticate with.
        password: String,
    },

    /// Authenticate with a private key.
    Key {
        /// The username to authenticate with.
        username: String,

        /// The optional path of the public key file.
        public_key: Option<PathBuf>,

        /// The path of the PEM-encoded private key file.
        private_key: PathBuf,

        /// The optional password used to decrypt the private key.
        password: Option<String>,
    },

    /// Authenticate via the system SSH agent.
    Agent {
        /// The username to authenticate with.
        username: String,

        /// The comment of the public key to use.
        ///
        /// If this is `Some`, the first public key with a comment that matches this value will be
        /// used. Otherwise, the first public key found in the agent will be used.
        comment: Option<String>,
    },
}

/// The configuration for opening an [`SftpStore`].
///
/// [`SftpStore`]: crate::store::SftpStore
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-sftp")))]
pub struct SftpConfig {
    /// The host and socket to connect to.
    pub addr: SocketAddr,

    /// The authentication for the connection.
    pub auth: SftpAuth,

    /// The path of the store on the server.
    pub path: PathBuf,
}

impl OpenStore for SftpConfig {
    type Store = SftpStore;

    fn open(&self) -> crate::Result<Self::Store> {
        // Connect to the SSH server.
        let stream = TcpStream::connect(&self.addr)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        let mut session =
            Session::new().map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        session.set_tcp_stream(stream);
        session
            .handshake()
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        // Perform authentication.
        match &self.auth {
            SftpAuth::Password { username, password } => {
                session
                    .userauth_password(username, password)
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            }
            SftpAuth::Key {
                username,
                public_key,
                private_key,
                password,
            } => {
                session
                    .userauth_pubkey_file(
                        username,
                        public_key.as_ref().map(|path| path.as_path()),
                        private_key,
                        password.as_ref().map(|str| str.as_str()),
                    )
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            }
            SftpAuth::Agent { username, comment } => match comment {
                Some(comment) => {
                    let mut agent = session
                        .agent()
                        .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                    agent
                        .connect()
                        .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                    agent
                        .list_identities()
                        .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                    let identities = agent
                        .identities()
                        .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                    let key = identities
                        .iter()
                        .find(|key| key.comment() == comment)
                        .ok_or(anyhow!("No key with matching comment found in agent."))
                        .map_err(|error| crate::Error::Store(error))?;
                    agent
                        .userauth(username, key)
                        .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                }
                None => {
                    session
                        .userauth_agent(username)
                        .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
                }
            },
        }

        let sftp = session
            .sftp()
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        // Create the directories if they don't exist.
        let directories = &[
            self.path.to_owned(),
            self.path.join(BLOCKS_DIRECTORY),
            self.path.join(STAGING_DIRECTORY),
        ];
        for directory in directories {
            if sftp.stat(&directory).is_err() {
                sftp.mkdir(&directory, 0o755)
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            }
        }

        let version_path = self.path.join(VERSION_FILE);

        if sftp.stat(&version_path).is_ok() {
            // Read the version ID file.
            let mut version_file = sftp
                .open(&version_path)
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            let mut version_id = String::new();
            version_file.read_to_string(&mut version_id)?;

            // Verify the version ID.
            if version_id != CURRENT_VERSION {
                return Err(crate::Error::UnsupportedStore);
            }
        } else {
            // Write the version ID file.
            let mut version_file = sftp
                .create(&version_path)
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            version_file.write_all(CURRENT_VERSION.as_bytes())?;
        }

        Ok(SftpStore {
            sftp,
            path: self.path.clone(),
        })
    }
}

/// A `DataStore` which stores data on an SFTP server.
///
/// You can use [`SftpConfig`] to open a data store of this type.
///
/// [`SftpConfig`]: crate::store::SftpConfig
#[cfg_attr(docsrs, doc(cfg(feature = "store-sftp")))]
pub struct SftpStore {
    sftp: Sftp,
    path: PathBuf,
}

impl SftpStore {
    /// Return the path where a block with the given `id` will be stored.
    fn block_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.path.join(BLOCKS_DIRECTORY).join(&hex[..2]).join(hex)
    }

    /// Return the path where a block with the given `id` will be staged.
    fn staging_path(&self, id: Uuid) -> PathBuf {
        let mut buffer = Uuid::encode_buffer();
        let hex = id.to_simple().encode_lower(&mut buffer);
        self.path.join(STAGING_DIRECTORY).join(hex)
    }

    /// Return whether the given remote `path` exists.
    fn exists(&self, path: &Path) -> bool {
        self.sftp.stat(path).is_ok()
    }
}

impl DataStore for SftpStore {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> anyhow::Result<()> {
        let staging_path = self.staging_path(id);
        let block_path = self.block_path(id);

        // If this is the first block its sub-directory, the directory needs to be created.
        let parent = block_path.parent().unwrap();
        if !self.exists(&parent) {
            self.sftp.mkdir(&parent, 0o755)?;
        }

        // Write to a staging file and then atomically move it to its final destination.
        let mut staging_file = self.sftp.create(&staging_path)?;
        staging_file.write_all(data)?;
        self.sftp.rename(
            &staging_path,
            &block_path,
            Some(RenameFlags::ATOMIC | RenameFlags::OVERWRITE),
        )?;

        // Remove any unused staging files.
        for (path, _) in self.sftp.readdir(&self.path.join(STAGING_DIRECTORY))? {
            self.sftp.unlink(&path)?;
        }

        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        let block_path = self.block_path(id);

        if !self.exists(&block_path) {
            return Ok(None);
        }

        let mut file = self.sftp.open(&block_path)?;

        let mut buffer = Vec::with_capacity(file.stat()?.size.unwrap_or(0) as usize);
        file.read_to_end(&mut buffer)?;
        Ok(Some(buffer))
    }

    fn remove_block(&mut self, id: Uuid) -> anyhow::Result<()> {
        let block_path = self.block_path(id);

        if !self.exists(&block_path) {
            return Ok(());
        }

        self.sftp.unlink(&block_path)?;

        Ok(())
    }

    fn list_blocks(&mut self) -> anyhow::Result<Vec<Uuid>> {
        let block_directories = self.sftp.readdir(&self.path.join(BLOCKS_DIRECTORY))?;
        let mut block_ids = Vec::new();

        for (block_directory, _) in block_directories {
            for (block_path, _) in self.sftp.readdir(&block_directory)? {
                let file_name = block_path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .expect("Block file name is invalid.");
                let id = Uuid::parse_str(file_name).expect("Block file name is invalid.");
                block_ids.push(id);
            }
        }

        Ok(block_ids)
    }
}

impl Debug for SftpStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SftpStore {{ path: {:?} }}", self.path)
    }
}
