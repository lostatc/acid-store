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

#[cfg(feature = "store-sftp")]
use acid_store::store::RcloneStore;
use rand::rngs::SmallRng;
use rand::{Rng, RngCore, SeedableRng};
#[cfg(feature = "store-directory")]
use {acid_store::store::DirectoryStore, std::path::Path};
#[cfg(feature = "store-redis")]
use {
    acid_store::store::RedisStore,
    redis::{Client as RedisClient, IntoConnectionInfo},
};
#[cfg(feature = "store-s3")]
use {
    acid_store::store::S3Store, s3::bucket::Bucket, s3::credentials::Credentials,
    s3::region::Region,
};
#[cfg(feature = "store-sftp")]
use {acid_store::store::SftpStore, ssh2::Session, std::net::TcpStream, std::path::PathBuf};
#[cfg(feature = "store-sqlite")]
use {acid_store::store::SqliteStore, rusqlite::Connection as SqliteConnection, std::path::Path};

use acid_store::repo::{Chunking, Compression, Encryption, RepoConfig};
use acid_store::store::DataStore;
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

/// Remove all blocks in the given `store`.
pub fn truncate_store(store: &mut impl DataStore) -> anyhow::Result<()> {
    for block_id in store.list_blocks()? {
        store.remove_block(block_id)?;
    }
    Ok(())
}

#[cfg(feature = "store-directory")]
pub fn directory_store(directory: &Path) -> anyhow::Result<DirectoryStore> {
    let mut store = DirectoryStore::new(directory.join("store"))?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-sqlite")]
pub fn sqlite_store(directory: &Path) -> anyhow::Result<SqliteStore> {
    let connection = SqliteConnection::open(directory.join("store.db"))?;
    let mut store = SqliteStore::new(connection)?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-redis")]
pub fn redis_store() -> anyhow::Result<RedisStore> {
    let info = dotenv::var("REDIS_URL").unwrap().into_connection_info()?;
    let client = RedisClient::open(info)?;
    let connection = client.get_connection()?;
    let mut store = RedisStore::new(connection)?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-s3")]
pub fn s3_store() -> anyhow::Result<S3Store> {
    let bucket = Bucket::new(
        &dotenv::var("S3_BUCKET").unwrap(),
        Region::UsEast1,
        Credentials::new(
            Some(dotenv::var("S3_ACCESS_KEY").unwrap()),
            Some(dotenv::var("S3_SECRET_KEY").unwrap()),
            None,
            None,
        ),
    )?;
    let mut store = S3Store::new(bucket, "test")?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-sftp")]
pub fn sftp_store() -> anyhow::Result<SftpStore> {
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

    let mut store = SftpStore::new(session.sftp()?, PathBuf::from(sftp_path))?;
    truncate_store(&mut store)?;
    Ok(store)
}

#[cfg(feature = "store-rclone")]
pub fn rclone_store() -> anyhow::Result<RcloneStore> {
    let mut store = RcloneStore::new(dotenv::var("RCLONE_REMOTE").unwrap())?;
    truncate_store(&mut store)?;
    Ok(store)
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
