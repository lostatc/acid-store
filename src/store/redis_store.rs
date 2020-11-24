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

#![cfg(feature = "store-redis")]

use std::fmt::{self, Debug, Formatter};
use std::path::PathBuf;

use redis::{Client, Commands, Connection, ConnectionAddr, ConnectionInfo, IntoConnectionInfo};
use uuid::Uuid;

use crate::store::common::DataStore;

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: &str = "b733bd82-4206-11ea-a3dc-7354076bdaf9";

/// The address for a Redis connection.
///
/// The `store-redis` cargo feature is required to use this.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RedisAddr {
    /// A hostname and port.
    Tcp(String, u16),

    /// The path of a Unix socket.
    Unix(PathBuf),
}

/// The configuration for a Redis connection.
///
/// The `store-redis` cargo feature is required to use this.
#[derive(Debug, PartialEq, Eq, Clone)]
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

/// A `DataStore` which stores data on a Redis server.
///
/// The `store-redis` cargo feature is required to use this.
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
                    return Err(crate::Error::UnsupportedFormat);
                }
            }
            None => connection
                .set("version", CURRENT_VERSION)
                .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?,
        }

        Ok(RedisStore { connection })
    }

    /// Open or create a `RedisStore` from the given `config`.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `RedisStore` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn new(config: RedisConfig) -> crate::Result<Self> {
        let info = ConnectionInfo {
            addr: Box::new(match config.addr {
                RedisAddr::Tcp(host, port) => ConnectionAddr::Tcp(host, port),
                RedisAddr::Unix(path) => ConnectionAddr::Unix(path),
            }),
            db: config.db,
            username: config.username,
            passwd: config.password,
        };
        Self::from_connection_info(info)
    }

    /// Open or create a `RedisStore` from a URL.
    ///
    /// For a TCP connection: `redis://[<username>][:<passwd>@]<hostname>[:port][/<db>]`
    ///
    /// For a Unix socket: `redis+unix:///[:<passwd>@]<path>[?db=<db>]`
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `RedisStore` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn from_url(url: &str) -> crate::Result<Self> {
        let info = url
            .into_connection_info()
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
        Self::from_connection_info(info)
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
