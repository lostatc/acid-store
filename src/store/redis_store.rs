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

#![cfg(feature = "store-redis")]

use std::fmt::{self, Debug, Formatter};
use std::path::PathBuf;

use redis::{Client, Commands, Connection, ConnectionAddr, ConnectionInfo, IntoConnectionInfo};
use uuid::Uuid;

use super::data_store::DataStore;
use super::open_store::OpenStore;

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: &str = "b733bd82-4206-11ea-a3dc-7354076bdaf9";

/// The address for a Redis connection.
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-redis")))]
pub enum RedisAddr {
    /// A hostname and port.
    Tcp(String, u16),

    /// The path of a Unix socket.
    Unix(PathBuf),
}

/// The configuration for opening a [`RedisStore`].
///
/// [`RedisStore`]: crate::store::RedisStore
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-redis")))]
pub struct RedisConfig {
    /// The address to connect to.
    pub addr: RedisAddr,

    /// The database number to use. This is usually `0`.
    pub db: i64,

    /// The optional username to use for the connection.
    pub username: Option<String>,

    /// The optional password to use for the connection.
    pub password: Option<String>,
}

impl RedisConfig {
    /// Construct a `RedisConfig` from a `url`.
    ///
    /// This returns `None` if the URL is invalid.
    ///
    /// For a TCP connection, the URL format is:
    /// `redis://[<username>][:<passwd>@]<hostname>[:port][/<db>]`.
    ///
    /// For a Unix socket connection, the URL format is:
    /// `redis+unix:///<path>[?db=<db>[&pass=<password>][&user=<username>]]`.
    pub fn from_url(url: &str) -> Option<Self> {
        let connection_info = url.into_connection_info().ok()?;
        Some(RedisConfig {
            addr: match *connection_info.addr {
                ConnectionAddr::Tcp(host, port) => RedisAddr::Tcp(host, port),
                ConnectionAddr::TcpTls { host, port, .. } => RedisAddr::Tcp(host, port),
                ConnectionAddr::Unix(path) => RedisAddr::Unix(path),
            },
            db: connection_info.db,
            username: connection_info.username,
            password: connection_info.passwd,
        })
    }
}

impl OpenStore for RedisConfig {
    type Store = RedisStore;

    fn open(&self) -> crate::Result<Self::Store> {
        let info = ConnectionInfo {
            addr: Box::new(match self.addr.clone() {
                RedisAddr::Tcp(host, port) => ConnectionAddr::Tcp(host, port),
                RedisAddr::Unix(path) => ConnectionAddr::Unix(path),
            }),
            db: self.db.clone(),
            username: self.username.clone(),
            passwd: self.password.clone(),
        };
        RedisStore::from_connection_info(info)
    }
}

/// A `DataStore` which stores data on a Redis server.
///
/// You can use [`RedisConfig`] to open a data store of this type.
///
/// [`RedisConfig`]: crate::store::RedisConfig
#[cfg_attr(docsrs, doc(cfg(feature = "store-redis")))]
pub struct RedisStore {
    connection: Connection,
}

impl Debug for RedisStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RedisStore")
    }
}

impl RedisStore {
    fn from_connection_info(info: ConnectionInfo) -> crate::Result<Self> {
        let mut connection = Client::open(info)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?
            .get_connection()
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let version_response: Option<String> = connection
            .get("version")
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        match version_response {
            Some(version) => {
                if version != CURRENT_VERSION {
                    return Err(crate::Error::UnsupportedStore);
                }
            }
            None => connection
                .set("version", CURRENT_VERSION)
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?,
        }

        Ok(RedisStore { connection })
    }
}

impl DataStore for RedisStore {
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> anyhow::Result<()> {
        let key_id = id.to_hyphenated().to_string();
        self.connection.set(format!("block:{}", key_id), data)?;
        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        let key_id = id.to_hyphenated().to_string();
        Ok(self.connection.get(format!("block:{}", key_id))?)
    }

    fn remove_block(&mut self, id: Uuid) -> anyhow::Result<()> {
        let key_id = id.to_hyphenated().to_string();
        self.connection.del(format!("block:{}", key_id))?;
        Ok(())
    }

    fn list_blocks(&mut self) -> anyhow::Result<Vec<Uuid>> {
        let blocks = self
            .connection
            .keys::<_, Vec<String>>("block:*")?
            .iter()
            .map(|key| {
                let uuid = key.trim_start_matches("block:");
                Uuid::parse_str(uuid).expect("Could not parse UUID.")
            })
            .collect();
        Ok(blocks)
    }
}
