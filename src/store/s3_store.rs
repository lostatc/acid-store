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

#![cfg(feature = "store-s3")]

use hex_literal::hex;
use s3::bucket::Bucket;
use s3::error::S3Error;
use uuid::Uuid;

use super::common::DataStore;

/// The separator to use in S3 object keys.
const SEPARATOR: &str = "/";

/// The key of the object which stores the repository version.
const VERSION_KEY: &str = "version";

/// The key prefix for block objects.
const BLOCK_PREFIX: &str = "block";

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: Uuid = Uuid::from_bytes(hex!("a2b7bda8 45ea 11ea ad75 afa592f123ef"));

/// The MIME content type to use for binary data.
const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

/// The HTTP status code for an object which does not exist.
const NOT_FOUND_CODE: u16 = 404;

/// Join the given segments into an S3 object key.
macro_rules! join_key {
    ($($segment:expr),*) => {
        {
            let mut path = String::new();
            $(
                path.push_str(&$segment);
                path.push_str(SEPARATOR);
            )*
            path.truncate(path.len() - SEPARATOR.len());
            path
        }
    }
}

/// A `DataStore` which stores data in an Amazon S3 bucket.
///
/// The `store-s3` cargo feature is required to use this.
#[derive(Debug)]
pub struct S3Store {
    bucket: Bucket,
    prefix: String,
}

impl S3Store {
    /// Open or create an `S3Store` in the given `bucket`.
    ///
    /// This accepts a `prefix` which is prepended to any keys created by the store. While keys
    /// in S3 are a flat namespace, you can think of this like the directory to create the store in.
    /// To create the store in the bucket root, pass an empty string.
    ///
    /// # Errors
    /// - `Error::UnsupportedFormat`: The repository is an unsupported format. This can mean that
    /// this is not a valid `S3Store` or this repository format is no longer supported by the
    /// library.
    /// - `Error::Store`: An error occurred with the data store.
    /// - `Error::Io`: An I/O error occurred.
    pub fn new(bucket: Bucket, prefix: &str) -> crate::Result<Self> {
        let prefix = prefix.trim_end_matches('/').to_owned();
        let version_key = join_key!(prefix, VERSION_KEY);

        match bucket.get_object(&version_key) {
            Ok((_, code)) if code == NOT_FOUND_CODE => {
                bucket
                    .put_object(
                        &version_key,
                        CURRENT_VERSION.as_bytes(),
                        BINARY_CONTENT_TYPE,
                    )
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            }
            Ok((version_bytes, _)) => {
                let version = Uuid::from_slice(version_bytes.as_slice())
                    .map_err(|_| crate::Error::UnsupportedFormat)?;
                if version != CURRENT_VERSION {
                    return Err(crate::Error::UnsupportedFormat);
                }
            }
            Err(error) => return Err(crate::Error::Store(anyhow::Error::from(error))),
        };

        Ok(S3Store { bucket, prefix })
    }

    /// Return the key of the block with the given `id`.
    fn block_path(&self, id: Uuid) -> String {
        join_key!(self.prefix, BLOCK_PREFIX, id.to_hyphenated().to_string())
    }
}

impl DataStore for S3Store {
    type Error = S3Error;

    fn write_block(&mut self, id: Uuid, data: &[u8]) -> Result<(), Self::Error> {
        let block_path = self.block_path(id);
        self.bucket
            .put_object(&block_path, data, BINARY_CONTENT_TYPE)?;
        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> Result<Option<Vec<u8>>, Self::Error> {
        let block_path = self.block_path(id);
        let (bytes, code) = self.bucket.get_object(&block_path)?;
        if code == NOT_FOUND_CODE {
            Ok(None)
        } else {
            Ok(Some(bytes))
        }
    }

    fn remove_block(&mut self, id: Uuid) -> Result<(), Self::Error> {
        let block_path = self.block_path(id);
        self.bucket.delete_object(&block_path)?;
        Ok(())
    }

    fn list_blocks(&mut self) -> Result<Vec<Uuid>, Self::Error> {
        let blocks_path = join_key!(self.prefix, BLOCK_PREFIX) + SEPARATOR;
        let block_ids = self
            .bucket
            .list_all(blocks_path.clone(), None)?
            .into_iter()
            .flat_map(|(list, _)| list.contents)
            .map(|object| {
                Uuid::parse_str(object.key.trim_start_matches(&blocks_path))
                    .expect("Could not parse UUID.")
            })
            .collect::<Vec<_>>();
        Ok(block_ids)
    }
}
