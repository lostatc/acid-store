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

#![allow(dead_code)]
#![cfg(all(feature = "encryption", feature = "compression"))]

use std::fmt::Debug;
use std::hash::Hash;

use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
#[cfg(feature = "store-redis")]
use redis::{ConnectionInfo, IntoConnectionInfo};
#[cfg(feature = "store-s3")]
use s3::bucket::Bucket;
#[cfg(feature = "store-s3")]
use s3::credentials::Credentials;
#[cfg(feature = "store-s3")]
use s3::region::Region;
#[cfg(feature = "store-sftp")]
use {acid_store::store::SftpConfig, ssh2::Session, std::net::TcpStream, std::path::PathBuf};

use acid_store::repo::{Chunking, Compression, Encryption, RepoConfig};
use lazy_static::lazy_static;

/// The minimum size of test data buffers.
pub const MIN_BUFFER_SIZE: usize = 1024;

/// The maximum size of test data buffers.
pub const MAX_BUFFER_SIZE: usize = 2048;

lazy_static! {
    /// The repository config to use for testing IO.
    pub static ref REPO_IO_CONFIG: RepoConfig = {
        let mut config = RepoConfig::default();
        config.chunking = Chunking::Zpaq { bits: 8 };
        config.encryption = Encryption::XChaCha20Poly1305;
        config.compression = Compression::Lz4 { level: 2 };
        config
    };
}

#[cfg(feature = "store-redis")]
pub fn redis_config() -> anyhow::Result<ConnectionInfo> {
    Ok(dotenv::var("REDIS_URL").unwrap().into_connection_info()?)
}

#[cfg(feature = "store-s3")]
pub fn s3_config() -> anyhow::Result<Bucket> {
    Ok(Bucket::new(
        &dotenv::var("S3_BUCKET").unwrap(),
        Region::UsEast1,
        Credentials::new(
            Some(dotenv::var("S3_ACCESS_KEY").unwrap()),
            Some(dotenv::var("S3_SECRET_KEY").unwrap()),
            None,
            None,
        ),
    )?)
}

#[cfg(feature = "store-sftp")]
pub fn sftp_config() -> anyhow::Result<SftpConfig> {
    let sftp_server: String = dotenv::var("SFTP_SERVER").unwrap();
    let sftp_path: String = dotenv::var("SFTP_PATH").unwrap();
    let sftp_username: String = dotenv::var("SFTP_USERNAME").unwrap();
    let sftp_password: String = dotenv::var("SFTP_PASSWORD").unwrap();

    let tcp = TcpStream::connect(sftp_server)?;
    let mut session = Session::new()?;
    session.set_tcp_stream(tcp);
    session.handshake()?;

    session.userauth_password(&sftp_username, &sftp_password)?;
    assert!(session.authenticated());

    Ok(SftpConfig {
        sftp: session.sftp()?,
        path: PathBuf::from(sftp_path),
    })
}

#[cfg(feature = "store-rclone")]
pub fn rclone_config() -> String {
    dotenv::var("RCLONE_REMOTE").unwrap()
}

/// Assert that two collections contain all the same elements, regardless of order.
pub fn assert_contains_all<T: Hash + Eq + Debug>(
    actual: impl IntoIterator<Item = T>,
    expected: impl IntoIterator<Item = T>,
) {
    assert_eq!(
        actual.into_iter().collect::<std::collections::HashSet<_>>(),
        expected
            .into_iter()
            .collect::<std::collections::HashSet<_>>()
    )
}

/// Return a buffer containing `size` random bytes for testing purposes.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    let mut buffer = vec![0u8; size];
    rng.fill_bytes(&mut buffer);
    buffer
}

/// Generate a random buffer of bytes of a random size.
pub fn random_buffer() -> Vec<u8> {
    let mut rng = SmallRng::from_entropy();
    random_bytes(rng.gen_range(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE))
}
