#![cfg(feature = "store-redis")]

use std::fmt::{self, Debug, Formatter};
use std::path::PathBuf;

use once_cell::sync::Lazy;
use redis::{
    Client, Commands, Connection, ConnectionAddr, ConnectionInfo, IntoConnectionInfo,
    RedisConnectionInfo,
};
use uuid::{uuid, Uuid};

use super::data_store::{BlockId, BlockKey, BlockType, DataStore};
use super::error::Result;
use super::open_store::OpenStore;

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: Uuid = uuid!("e64b73f0-f90f-11eb-bb25-7348383f5353");

static CURRENT_VERSION_STR: Lazy<&'static str> = Lazy::new(|| {
    CURRENT_VERSION
        .as_hyphenated()
        .encode_lower(&mut Uuid::encode_buffer())
});

const DATA_KEY: &str = "store:data";
const LOCKS_KEY: &str = "store:lock";
const HEADERS_KEY: &str = "store:header";
const SUPER_KEY: &str = "store:super";
const REPO_VERSION_KEY: &str = "store:version";
const STORE_VERSION_KEY: &str = "version";

fn block_key(key: BlockKey) -> String {
    match key {
        BlockKey::Data(id) => format!("{}:{}", DATA_KEY, id.as_ref().as_hyphenated()),
        BlockKey::Lock(id) => format!("{}:{}", LOCKS_KEY, id.as_ref().as_hyphenated()),
        BlockKey::Header(id) => format!("{}:{}", HEADERS_KEY, id.as_ref().as_hyphenated()),
        BlockKey::Super => SUPER_KEY.to_string(),
        BlockKey::Version => REPO_VERSION_KEY.to_string(),
    }
}

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
            addr: match connection_info.addr {
                ConnectionAddr::Tcp(host, port) => RedisAddr::Tcp(host, port),
                ConnectionAddr::TcpTls { host, port, .. } => RedisAddr::Tcp(host, port),
                ConnectionAddr::Unix(path) => RedisAddr::Unix(path),
            },
            db: connection_info.redis.db,
            username: connection_info.redis.username,
            password: connection_info.redis.password,
        })
    }
}

impl OpenStore for RedisConfig {
    type Store = RedisStore;

    fn open(&self) -> crate::Result<Self::Store> {
        let info = ConnectionInfo {
            addr: match self.addr.clone() {
                RedisAddr::Tcp(host, port) => ConnectionAddr::Tcp(host, port),
                RedisAddr::Unix(path) => ConnectionAddr::Unix(path),
            },
            redis: RedisConnectionInfo {
                db: self.db,
                username: self.username.clone(),
                password: self.password.clone(),
            },
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
        f.debug_struct("RedisStore").finish_non_exhaustive()
    }
}

impl RedisStore {
    fn from_connection_info(info: ConnectionInfo) -> crate::Result<Self> {
        let mut connection = Client::open(info)
            .map_err(|error| crate::Error::Store(error.into()))?
            .get_connection()
            .map_err(|error| crate::Error::Store(error.into()))?;

        let version_response: Option<String> = connection
            .get(STORE_VERSION_KEY)
            .map_err(|error| crate::Error::Store(error.into()))?;

        match version_response {
            Some(version) => {
                if version != *CURRENT_VERSION_STR {
                    return Err(crate::Error::UnsupportedStore);
                }
            }
            None => connection
                .set(STORE_VERSION_KEY, *CURRENT_VERSION_STR)
                .map_err(|error| crate::Error::Store(error.into()))?,
        }

        Ok(RedisStore { connection })
    }
}

impl DataStore for RedisStore {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> Result<()> {
        self.connection.set(block_key(key), data)?;
        Ok(())
    }

    fn read_block(&mut self, key: BlockKey, buf: &mut Vec<u8>) -> Result<Option<usize>> {
        let bytes: Vec<u8> = self.connection.get(block_key(key))?;
        buf.extend_from_slice(&bytes);
        Ok(Some(bytes.len()))
    }

    fn remove_block(&mut self, key: BlockKey) -> Result<()> {
        self.connection.del(block_key(key))?;
        Ok(())
    }

    fn list_blocks(&mut self, kind: BlockType, list: &mut Vec<BlockId>) -> Result<()> {
        let key_prefix = match kind {
            BlockType::Data => format!("{}:", DATA_KEY),
            BlockType::Lock => format!("{}:", LOCKS_KEY),
            BlockType::Header => format!("{}:", HEADERS_KEY),
        };
        let search_key = format!("{}*", key_prefix);

        let keys = self.connection.keys::<_, Vec<String>>(search_key)?;

        for key in keys {
            let uuid = key.trim_start_matches(&key_prefix);
            list.push(Uuid::parse_str(uuid).map(BlockId::from)?);
        }

        Ok(())
    }
}
