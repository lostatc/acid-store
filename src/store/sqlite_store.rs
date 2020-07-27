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

use hex_literal::hex;
use rusqlite::{params, Connection, OptionalExtension};
use uuid::Uuid;

use crate::store::common::DataStore;

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: Uuid = Uuid::from_bytes(hex!("08d14eb8 4156 11ea 8ec7 a31cc3dfe2e4"));

/// A `DataStore` which stores data in a SQLite database.
///
/// The `store-sqlite` cargo feature is required to use this.
#[derive(Debug)]
pub struct SqliteStore {
    /// The connection to the SQLite database.
    connection: Connection,
}

impl SqliteStore {
    /// Open or create a `SqliteStore` with the given database `connection`.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `SqliteStore` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn new(connection: Connection) -> crate::Result<Self> {
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
                    .map_err(|_| crate::Error::UnsupportedFormat)?;
                if version != CURRENT_VERSION {
                    return Err(crate::Error::UnsupportedFormat);
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

impl DataStore for SqliteStore {
    type Error = rusqlite::Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        self.connection.execute(
            r#"
                REPLACE INTO Blocks (uuid, data)
                VALUES (?1, ?2);
            "#,
            params![&id.as_bytes()[..], data],
        )?;

        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        self.connection
            .query_row(
                r#"
                    SELECT data FROM Blocks
                    WHERE uuid = ?1;
                "#,
                params![&id.as_bytes()[..]],
                |row| row.get(0),
            )
            .optional()
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        self.connection.execute(
            r#"
                DELETE FROM Blocks
                WHERE uuid = ?1;
            "#,
            params![&id.as_bytes()[..]],
        )?;

        Ok(())
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
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