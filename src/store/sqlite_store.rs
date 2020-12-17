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

#![cfg(feature = "store-sqlite")]

use std::path::PathBuf;

use hex_literal::hex;
use rusqlite::{params, Connection, OptionalExtension};
use uuid::Uuid;

use super::data_store::DataStore;
use super::open_store::OpenStore;

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: Uuid = Uuid::from_bytes(hex!("08d14eb8 4156 11ea 8ec7 a31cc3dfe2e4"));

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
                    CREATE TABLE IF NOT EXISTS Blocks (
                        uuid BLOB PRIMARY KEY,
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
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> anyhow::Result<()> {
        self.connection.execute(
            r#"
                REPLACE INTO Blocks (uuid, data)
                VALUES (?1, ?2);
            "#,
            params![&id.as_bytes()[..], data],
        )?;

        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self
            .connection
            .query_row(
                r#"
                    SELECT data FROM Blocks
                    WHERE uuid = ?1;
                "#,
                params![&id.as_bytes()[..]],
                |row| row.get(0),
            )
            .optional()?)
    }

    fn remove_block(&mut self, id: Uuid) -> anyhow::Result<()> {
        self.connection.execute(
            r#"
                DELETE FROM Blocks
                WHERE uuid = ?1;
            "#,
            params![&id.as_bytes()[..]],
        )?;

        Ok(())
    }

    fn list_blocks(&mut self) -> anyhow::Result<Vec<Uuid>> {
        let mut statement = self.connection.prepare(r#"SELECT uuid FROM Blocks;"#)?;

        let result = statement
            .query_map(params![], |row| {
                row.get(0).map(|bytes: Vec<u8>| {
                    Uuid::from_slice(bytes.as_slice()).expect("Could not parse UUID.")
                })
            })?
            .collect::<Result<Vec<Uuid>, _>>()?;

        Ok(result)
    }
}
