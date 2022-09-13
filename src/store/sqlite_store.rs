#![cfg(feature = "store-sqlite")]

use std::path::PathBuf;

use hex_literal::hex;
use rusqlite::{params, Connection, OptionalExtension, NO_PARAMS};
use uuid::Uuid;

use super::data_store::{BlockId, BlockKey, BlockType, DataStore};
use super::open_store::OpenStore;

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: Uuid = Uuid::from_bytes(hex!("42efde7c f927 11eb bb01 d70e242b02af"));

/// The configuration for opening a [`SqliteStore`].
///
/// [`SqliteStore`]: crate::store::SqliteStore
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-sqlite")))]
pub struct SqliteConfig {
    /// The path of the SQLite database.
    pub path: PathBuf,
}

impl OpenStore for SqliteConfig {
    type Store = SqliteStore;

    fn open(&self) -> crate::Result<Self::Store> {
        let connection = Connection::open(&self.path)
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        connection
            .execute_batch(
                r#"
                    CREATE TABLE IF NOT EXISTS Data (
                        uuid BLOB PRIMARY KEY,
                        data BLOB NOT NULL
                    );
                    
                    CREATE TABLE IF NOT EXISTS Locks (
                        uuid BLOB PRIMARY KEY,
                        data BLOB NOT NULL
                    );
                    
                    CREATE TABLE IF NOT EXISTS Headers (
                        uuid BLOB PRIMARY KEY,
                        data BLOB NOT NULL
                    );
                    
                    CREATE TABLE IF NOT EXISTS Blocks (
                        key TEXT PRIMARY KEY,
                        data BLOB NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS Metadata (
                        key TEXT PRIMARY KEY,
                        value BLOB NOT NULL
                    );
                "#,
            )
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        let version_bytes: Option<Vec<u8>> = connection
            .query_row(
                r#"
                    SELECT value FROM Metadata
                    WHERE key = 'version';
                "#,
                params![],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;

        match version_bytes {
            Some(bytes) => {
                let version = Uuid::from_slice(bytes.as_slice())
                    .map_err(|_| crate::Error::UnsupportedStore)?;
                if version != CURRENT_VERSION {
                    return Err(crate::Error::UnsupportedStore);
                }
            }
            None => {
                connection
                    .execute(
                        r#"
                        INSERT INTO Metadata (key, value)
                        VALUES ('version', ?1);
                    "#,
                        params![&CURRENT_VERSION.as_bytes()[..]],
                    )
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            }
        }

        Ok(SqliteStore { connection })
    }
}

/// A `DataStore` which stores data in a SQLite database.
///
/// You can use [`SqliteConfig`] to open a data store of this type.
///
/// [`SqliteConfig`]: crate::store::SqliteConfig
#[derive(Debug)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-sqlite")))]
pub struct SqliteStore {
    /// The connection to the SQLite database.
    connection: Connection,
}

impl DataStore for SqliteStore {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        match key {
            BlockKey::Data(id) => {
                self.connection.execute(
                    r#"
                        REPLACE INTO Data (uuid, data)
                        VALUES (?1, ?2);
                    "#,
                    params![&id.as_ref().as_bytes()[..], data],
                )?;
            }
            BlockKey::Lock(id) => {
                self.connection.execute(
                    r#"
                        REPLACE INTO Locks (uuid, data)
                        VALUES (?1, ?2);
                    "#,
                    params![&id.as_ref().as_bytes()[..], data],
                )?;
            }
            BlockKey::Header(id) => {
                self.connection.execute(
                    r#"
                        REPLACE INTO Headers (uuid, data)
                        VALUES (?1, ?2);
                    "#,
                    params![&id.as_ref().as_bytes()[..], data],
                )?;
            }
            BlockKey::Super => {
                self.connection.execute(
                    r#"
                        REPLACE INTO Blocks (key, data)
                        VALUES ('super', ?1);
                    "#,
                    params![data],
                )?;
            }
            BlockKey::Version => {
                self.connection.execute(
                    r#"
                        REPLACE INTO Blocks (key, data)
                        VALUES ('version', ?1);
                    "#,
                    params![data],
                )?;
            }
        }

        Ok(())
    }

    fn read_block(&mut self, key: BlockKey) -> anyhow::Result<Option<Vec<u8>>> {
        match key {
            BlockKey::Data(id) => Ok(self
                .connection
                .query_row(
                    r#"
                        SELECT data FROM Data
                        WHERE uuid = ?1;
                    "#,
                    params![&id.as_ref().as_bytes()[..]],
                    |row| row.get(0),
                )
                .optional()?),
            BlockKey::Lock(id) => Ok(self
                .connection
                .query_row(
                    r#"
                        SELECT data FROM Locks
                        WHERE uuid = ?1;
                    "#,
                    params![&id.as_ref().as_bytes()[..]],
                    |row| row.get(0),
                )
                .optional()?),
            BlockKey::Header(id) => Ok(self
                .connection
                .query_row(
                    r#"
                        SELECT data FROM Headers
                        WHERE uuid = ?1;
                    "#,
                    params![&id.as_ref().as_bytes()[..]],
                    |row| row.get(0),
                )
                .optional()?),
            BlockKey::Super => Ok(self
                .connection
                .query_row(
                    r#"
                        SELECT data FROM Blocks
                        WHERE key = 'super';
                    "#,
                    NO_PARAMS,
                    |row| row.get(0),
                )
                .optional()?),
            BlockKey::Version => Ok(self
                .connection
                .query_row(
                    r#"
                        SELECT data FROM Blocks
                        WHERE key = 'version';
                    "#,
                    NO_PARAMS,
                    |row| row.get(0),
                )
                .optional()?),
        }
    }

    fn remove_block(&mut self, key: BlockKey) -> anyhow::Result<()> {
        match key {
            BlockKey::Data(id) => {
                self.connection.execute(
                    r#"
                        DELETE FROM Data
                        WHERE uuid = ?1;
                    "#,
                    params![&id.as_ref().as_bytes()[..]],
                )?;
            }
            BlockKey::Lock(id) => {
                self.connection.execute(
                    r#"
                        DELETE FROM Locks
                        WHERE uuid = ?1;
                    "#,
                    params![&id.as_ref().as_bytes()[..]],
                )?;
            }
            BlockKey::Header(id) => {
                self.connection.execute(
                    r#"
                        DELETE FROM Headers
                        WHERE uuid = ?1;
                    "#,
                    params![&id.as_ref().as_bytes()[..]],
                )?;
            }
            BlockKey::Super => {
                self.connection.execute(
                    r#"
                        DELETE FROM Blocks
                        WHERE key = 'super';
                    "#,
                    NO_PARAMS,
                )?;
            }
            BlockKey::Version => {
                self.connection.execute(
                    r#"
                        DELETE FROM Blocks
                        WHERE key = 'version';
                    "#,
                    NO_PARAMS,
                )?;
            }
        }

        Ok(())
    }

    fn list_blocks(&mut self, kind: BlockType) -> anyhow::Result<Vec<BlockId>> {
        let mut statement = match kind {
            BlockType::Data => self.connection.prepare(r#"SELECT uuid FROM Data;"#)?,
            BlockType::Lock => self.connection.prepare(r#"SELECT uuid FROM Locks;"#)?,
            BlockType::Header => self.connection.prepare(r#"SELECT uuid FROM Headers;"#)?,
        };

        let result = statement
            .query_map(params![], |row| row.get::<_, Vec<u8>>(0))?
            .collect::<Result<Vec<Vec<u8>>, _>>()?
            .into_iter()
            .map(|id_bytes| Uuid::from_slice(id_bytes.as_slice()).map(|id| id.into()))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(result)
    }
}
