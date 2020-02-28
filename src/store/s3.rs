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

#![cfg(feature = "store-s3")]

use s3::bucket::Bucket;
use s3::error::S3Error;
use uuid::Uuid;

use lazy_static::lazy_static;

use super::common::{DataStore, Open, OpenOption};

lazy_static! {
    /// A UUID which acts as the version ID of the store format.
    static ref CURRENT_VERSION: Uuid =
        Uuid::parse_str("a2b7bda8-45ea-11ea-ad75-afa592f123ef").unwrap();
}

/// The MIME content type to use for binary data.
const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

/// HTTP status codes.
const NOT_FOUND_CODE: u16 = 404;

/// A `DataStore` which stores data in an Amazon S3 bucket.
#[derive(Debug)]
pub struct S3Store {
    bucket: Bucket,
}

impl Open for S3Store {
    type Config = Bucket;

    fn open(config: Self::Config, options: OpenOption) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let (version_bytes, _) = config.get_object("version").map_err(anyhow::Error::from)?;
        let version = Uuid::from_slice(version_bytes.as_slice()).ok();

        dbg!(version);
        dbg!(*CURRENT_VERSION);

        match version {
            Some(version) if version == *CURRENT_VERSION => {
                if options.contains(OpenOption::CREATE_NEW) {
                    return Err(crate::Error::AlreadyExists);
                }
            }
            _ => {
                if options.intersects(OpenOption::CREATE | OpenOption::CREATE_NEW) {
                    config
                        .put_object("version", CURRENT_VERSION.as_bytes(), BINARY_CONTENT_TYPE)
                        .map_err(anyhow::Error::from)?;
                } else {
                    return Err(crate::Error::UnsupportedFormat);
                }
            }
        }

        if options.contains(OpenOption::TRUNCATE) {
            let block_paths = config
                .list_all(String::from("block/"), None)
                .map_err(anyhow::Error::from)?
                .into_iter()
                .flat_map(|(list, _)| list.contents)
                .map(|object| object.key);
            for block_path in block_paths {
                config
                    .delete_object(&block_path)
                    .map_err(anyhow::Error::from)?;
            }
        }

        Ok(Self { bucket: config })
    }
}

impl DataStore for S3Store {
    type Error = S3Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let block_path = format!("block/{}", id.to_hyphenated().to_string());
        self.bucket
            .put_object(&block_path, data, BINARY_CONTENT_TYPE)?;
        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let block_path = format!("block/{}", id.to_hyphenated().to_string());
        let (bytes, code) = self.bucket.get_object(&block_path)?;
        if code == NOT_FOUND_CODE {
            Ok(None)
        } else {
            Ok(Some(bytes))
        }
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let block_path = format!("block/{}", id.to_hyphenated().to_string());
        self.bucket.delete_object(&block_path)?;
        Ok(())
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        let block_ids = self
            .bucket
            .list_all(String::from("block/"), None)?
            .into_iter()
            .flat_map(|(list, _)| list.contents)
            .map(|object| {
                Uuid::parse_str(object.key.trim_start_matches("block/"))
                    .expect("Could not parse UUID.")
            })
            .collect::<Vec<_>>();
        Ok(block_ids)
    }
}
