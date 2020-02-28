/*
 * Copyright 2019-2020 Garrett Powell
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

use redis::{Client, Commands, Connection, ConnectionInfo, RedisError};
use uuid::Uuid;

use crate::store::common::{DataStore, Open, OpenOption};

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: &str = "b733bd82-4206-11ea-a3dc-7354076bdaf9";

/// A `DataStore` which stores data on a Redis server.
pub struct RedisStore {
    connection: Connection,
}

impl Debug for RedisStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "RedisStore")
    }
}

impl Open for RedisStore {
    type Config = ConnectionInfo;

    fn open(config: Self::Config, options: OpenOption) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let client = Client::open(config).map_err(anyhow::Error::from)?;
        let mut connection = client.get_connection().map_err(anyhow::Error::from)?;

        let version_response: Option<String> =
            connection.get("version").map_err(anyhow::Error::from)?;

        match version_response {
            Some(version) if version == *CURRENT_VERSION => {
                if options.contains(OpenOption::CREATE_NEW) {
                    return Err(crate::Error::AlreadyExists);
                }
            }
            _ => {
                if options.intersects(OpenOption::CREATE | OpenOption::CREATE_NEW) {
                    connection
                        .set("version", CURRENT_VERSION)
                        .map_err(anyhow::Error::from)?;
                } else {
                    return Err(crate::Error::UnsupportedFormat);
                }
            }
        }

        if options.contains(OpenOption::TRUNCATE) {
            let keys = connection
                .keys::<_, Vec<String>>("block:*")
                .map_err(anyhow::Error::from)?;
            for key in keys {
                connection.del(key).map_err(anyhow::Error::from)?;
            }
        }

        Ok(Self { connection })
    }
}

impl DataStore for RedisStore {
    type Error = RedisError;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let key_id = id.to_hyphenated().to_string();
        self.connection.set(format!("block:{}", key_id), data)?;
        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let key_id = id.to_hyphenated().to_string();
        self.connection.get(format!("block:{}", key_id))
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let key_id = id.to_hyphenated().to_string();
        self.connection.del(format!("block:{}", key_id))?;
        Ok(())
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
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
