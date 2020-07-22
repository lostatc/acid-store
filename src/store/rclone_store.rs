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

#![cfg(all(unix, feature = "store-rclone"))]

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use rand::distributions::Alphanumeric;
use rand::Rng;
use secrecy::{ExposeSecret, Secret, SecretString};
use ssh2::Session;
use uuid::Uuid;

use crate::store::{DataStore, SftpStore};

/// Generate a random secure password for the SFTP server.
fn generate_password(length: u32) -> SecretString {
    Secret::new(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length as usize)
            .collect(),
    )
}

/// Return an unused ephemeral port number.
fn ephemeral_port() -> io::Result<u16> {
    match UdpSocket::bind("localhost:0")?.local_addr()? {
        SocketAddr::V4(address) => Ok(address.port()),
        SocketAddr::V6(address) => Ok(address.port()),
    }
}

/// The length of the password for the SFTP server.
const PASSWORD_LENGTH: u32 = 30;

/// The username for authenticating the SSH connection.
const SSH_USERNAME: &str = "rclone";

/// The amount of time to wait between attempts to connect to the SFTP server.
const CONNECT_WAIT_TIME: Duration = Duration::from_millis(100);

/// Serve the rclone remote over SFTP and return the server process.
fn serve(port: u16, password: &SecretString, config: &str) -> io::Result<Child> {
    Command::new("rclone")
        .args(&[
            "serve",
            "sftp",
            "--vfs-cache-mode",
            "writes",
            "--addr",
            &format!("localhost:{}", port),
            "--user",
            SSH_USERNAME,
            "--pass",
            password.expose_secret(),
            config,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
}

/// A `DataStore` which stores data in cloud storage using rclone.
///
/// The `store-rclone` cargo feature is required to use this.
///
/// This is a data store which is backed by [rclone](https://rclone.org/), allowing access to a wide
/// variety of cloud storage providers.
///
/// To use this data store, rclone must be installed and available on the `PATH`. Rclone version
/// 1.48.0 or higher is required.
#[derive(Debug)]
pub struct RcloneStore {
    sftp_store: SftpStore,
    server_process: Child,
}

impl RcloneStore {
    /// Open or create an `RcloneStore`.
    ///
    /// This accepts a `config` value, which is a string with the format `<remote>:<path>`, where
    /// `<remote>` is the name of the remote as configured using `rclone config` and `<path>` is
    /// the path of the directory on the remote to use.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `RcloneStore` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn new(config: String) -> crate::Result<Self> {
        // Serve the rclone remote over SFTP.
        let port = ephemeral_port()?;
        let password = generate_password(PASSWORD_LENGTH);
        let server_process = serve(port, &password, &config)?;

        // Attempt to connect to the SFTP server while we wait for it to start up.
        let tcp_stream: TcpStream;
        loop {
            match TcpStream::connect(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)) {
                Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
                    sleep(CONNECT_WAIT_TIME);
                    continue;
                }
                Err(error) => return Err(error.into()),
                Ok(tcp) => {
                    tcp_stream = tcp;
                    break;
                }
            }
        }

        // Connect the SFTP client.
        let mut session =
            Session::new().map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        session.set_tcp_stream(tcp_stream);
        session
            .handshake()
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        session
            .userauth_password(SSH_USERNAME, password.expose_secret())
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        let sftp = session
            .sftp()
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let sftp_store = SftpStore::new(sftp, PathBuf::from(""))?;

        Ok(Self {
            sftp_store,
            server_process,
        })
    }
}

impl DataStore for RcloneStore {
    type Error = io::Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        self.sftp_store.write_block(id, data)
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        self.sftp_store.read_block(id)
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        self.sftp_store.remove_block(id)
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        self.sftp_store.list_blocks()
    }
}

impl Drop for RcloneStore {
    fn drop(&mut self) {
        self.server_process.kill().ok();
    }
}
