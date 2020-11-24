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

use std::env;

use hex_literal::hex;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use tokio::runtime::Runtime;
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

/// The environment variable for the AWS access key.
const ACCESS_KEY_ENV: &str = "AWS_ACCESS_KEY_ID";

/// The environment variable for the AWS secrete key.
const SECRET_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";

/// The environment variable for the AWS session token.
const SESSION_TOKEN_ENV: &str = "AWS_SESSION_TOKEN";

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

/// An AWS region.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum S3Region {
    /// us-east-1
    UsEast1,

    /// us-east-2
    UsEast2,

    /// us-west-1
    UsWest1,

    /// us-west-2
    UsWest2,

    /// af-south-1
    AfSouth1,

    /// ap-east-1
    ApEast1,

    /// ap-south-1
    ApSouth1,

    /// ap-northeast-1
    ApNortheast1,

    /// ap-northeast-2
    ApNortheast2,

    /// ap-northeast-3
    ApNortheast3,

    /// ap-southeast-1
    ApSoutheast1,

    /// ap-southeast-2
    ApSoutheast2,

    /// ca-central-1
    CaCentral1,

    /// cn-north-1
    CnNorth1,

    /// cn-northwest-1
    CnNorthwest1,

    /// eu-central-1
    EuCentral1,

    /// eu-west-1
    EuWest1,

    /// eu-west-2
    EuWest2,

    /// eu-west-3
    EuWest3,

    /// eu-south-1
    EuSouth1,

    /// eu-north-1
    EuNorth1,

    /// me-south-1
    MeSouth1,

    /// sa-east-1
    SaEast1,

    /// Custom region
    Custom { name: String, endpoint: String },
}

impl S3Region {
    /// Return the S3 region with the given `name` or `None` if there is none.
    pub fn from_name(name: &str) -> Option<S3Region> {
        use S3Region::*;
        Some(match name {
            "us-east-1" => UsEast1,
            "us-east-2" => UsEast2,
            "us-west-1" => UsWest1,
            "us-west-2" => UsWest2,
            "af-south-1" => AfSouth1,
            "ap-east-1" => ApEast1,
            "ap-south-1" => ApSouth1,
            "ap-northeast-1" => ApNortheast1,
            "ap-northeast-2" => ApNortheast2,
            "ap-northeast-3" => ApNortheast3,
            "ap-southeast-1" => ApSoutheast1,
            "ap-southeast-2" => ApSoutheast2,
            "ca-central-1" => CaCentral1,
            "cn-north-1" => CnNorth1,
            "cn-northwest-1" => CnNorthwest1,
            "eu-central-1" => EuCentral1,
            "eu-west-1" => EuWest1,
            "eu-west-2" => EuWest2,
            "eu-west-3" => EuWest3,
            "eu-south-1" => EuSouth1,
            "eu-north-1" => EuNorth1,
            "me-south-1" => MeSouth1,
            "sa-east-1" => SaEast1,
            _ => return None,
        })
    }

    /// The name of this AWS region.
    pub fn name(&self) -> &str {
        use S3Region::*;
        match self {
            UsEast1 => "us-east-1",
            UsEast2 => "us-east-2",
            UsWest1 => "us-west-1",
            UsWest2 => "us-west-2",
            AfSouth1 => "af-south-1",
            ApEast1 => "ap-east-1",
            ApSouth1 => "ap-south-1",
            ApNortheast1 => "ap-northeast-1",
            ApNortheast2 => "ap-northeast-2",
            ApNortheast3 => "ap-northeast-3",
            ApSoutheast1 => "ap-southeast-1",
            ApSoutheast2 => "ap-southeast-2",
            CaCentral1 => "ca-central-1",
            CnNorth1 => "cn-north-1",
            CnNorthwest1 => "cn-northwest-1",
            EuCentral1 => "eu-central-1",
            EuWest1 => "eu-west-1",
            EuWest2 => "eu-west-2",
            EuWest3 => "eu-west-3",
            EuSouth1 => "eu-south-1",
            EuNorth1 => "eu-north-1",
            MeSouth1 => "me-south-1",
            SaEast1 => "sa-east-1",
            Custom { name, .. } => name,
        }
    }

    /// The S3 endpoint of this AWS region.
    pub fn endpoint(&self) -> &str {
        use S3Region::*;
        match self {
            UsEast1 => "https://s3.us-east-1.amazonaws.com",
            UsEast2 => "https://s3.us-east-2.amazonaws.com",
            UsWest1 => "https://s3.us-west-1.amazonaws.com",
            UsWest2 => "https://s3.us-west-2.amazonaws.com",
            AfSouth1 => "https://s3.af-south-1.amazonaws.com",
            ApEast1 => "https://s3.ap-east-1.amazonaws.com",
            ApSouth1 => "https://s3.ap-south-1.amazonaws.com",
            ApNortheast1 => "https://s3.ap-northeast-1.amazonaws.com",
            ApNortheast2 => "https://s3.ap-northeast-2.amazonaws.com",
            ApNortheast3 => "https://s3.ap-northeast-3.amazonaws.com",
            ApSoutheast1 => "https://s3.ap-southeast-1.amazonaws.com",
            ApSoutheast2 => "https://s3.ap-southeast-2.amazonaws.com",
            CaCentral1 => "https://s3.ca-central-1.amazonaws.com",
            CnNorth1 => "https://s3.cn-north-1.amazonaws.com",
            CnNorthwest1 => "https://s3.cn-northwest-1.amazonaws.com",
            EuCentral1 => "https://s3.eu-central-1.amazonaws.com",
            EuWest1 => "https://s3.eu-west-1.amazonaws.com",
            EuWest2 => "https://s3.eu-west-2.amazonaws.com",
            EuWest3 => "https://s3.eu-west-3.amazonaws.com",
            EuSouth1 => "https://s3.eu-south-1.amazonaws.com",
            EuNorth1 => "https://s3.eu-north-1.amazonaws.com",
            MeSouth1 => "https://s3.me-south-1.amazonaws.com",
            SaEast1 => "https://s3.sa-east-1.amazonaws.com",
            Custom { endpoint, .. } => endpoint,
        }
    }
}

/// The credentials for an S3 connection.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum S3Credentials {
    /// Anonymous credentials for accessing public objects.
    Anonymous,

    /// Basic credentials.
    Basic {
        access_key: String,
        secret_key: String,
    },

    /// Session credentials.
    Session {
        access_key: String,
        secret_key: String,
        session_token: String,
    },
}

impl S3Credentials {
    /// Get `S3Credentials` from environment variables.
    ///
    /// This checks the following environment variables:
    /// - `AWS_ACCESS_KEY_ID`
    /// - `AWS_SECRET_ACCESS_KEY`
    /// - `AWS_SESSION_TOKEN`
    ///
    /// This returns `None` if the environment variables were unset or malformed.
    pub fn from_env() -> Option<Self> {
        let access_key = env::var(ACCESS_KEY_ENV).ok()?;
        let secret_key = env::var(SECRET_KEY_ENV).ok()?;
        Some(match env::var(SESSION_TOKEN_ENV).ok() {
            Some(session_token) => S3Credentials::Session {
                access_key,
                secret_key,
                session_token,
            },
            None => S3Credentials::Basic {
                access_key,
                secret_key,
            },
        })
    }

    /// Get `S3Credentials` from the profile with the given `name`.
    ///
    /// This returns `None` if the profile was not found or could not be read.
    pub fn from_profile(name: &str) -> Option<Self> {
        let credentials = Credentials::from_profile(Some(name)).ok()?;
        let access_key = credentials.access_key?;
        let secret_key = credentials.secret_key?;
        Some(match credentials.session_token {
            Some(session_token) => S3Credentials::Session {
                access_key,
                secret_key,
                session_token,
            },
            None => S3Credentials::Basic {
                access_key,
                secret_key,
            },
        })
    }
}

/// The configuration for an S3 connection.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct S3Config {
    /// The name of the S3 bucket.
    pub bucket: String,

    /// The AWS region to connect to.
    pub region: S3Region,

    /// The credentials to connect with.
    pub credentials: S3Credentials,
}

impl S3Config {
    fn into_bucket(self) -> Bucket {
        Bucket::new(
            self.bucket.as_str(),
            Region::Custom {
                region: self.region.name().to_string(),
                endpoint: self.region.endpoint().to_string(),
            },
            match self.credentials {
                S3Credentials::Anonymous => Credentials::anonymous().unwrap(),
                S3Credentials::Basic {
                    access_key,
                    secret_key,
                } => Credentials {
                    access_key: Some(access_key),
                    secret_key: Some(secret_key),
                    security_token: None,
                    session_token: None,
                },
                S3Credentials::Session {
                    access_key,
                    secret_key,
                    session_token,
                } => Credentials {
                    access_key: Some(access_key),
                    secret_key: Some(secret_key),
                    security_token: None,
                    session_token: Some(session_token),
                },
            },
        )
        .unwrap()
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
    /// Open or create an `S3Store` from the given `config`.
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
    pub fn new(config: S3Config, prefix: &str) -> crate::Result<Self> {
        let bucket = config.into_bucket();
        let prefix = prefix.trim_end_matches('/').to_owned();
        let version_key = join_key!(prefix, VERSION_KEY);
        let mut runtime = Runtime::new().unwrap();

        match runtime.block_on(bucket.get_object(&version_key)) {
            Ok((_, code)) if code == NOT_FOUND_CODE => {
                runtime
                    .block_on(bucket.put_object(&version_key, CURRENT_VERSION.as_bytes()))
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
    fn write_block(&mut self, id: Uuid, data: &[u8]) -> anyhow::Result<()> {
        let mut runtime = Runtime::new().unwrap();

        let block_path = self.block_path(id);
        runtime.block_on(self.bucket.put_object(&block_path, data))?;
        Ok(())
    }

    fn read_block(&mut self, id: Uuid) -> anyhow::Result<Option<Vec<u8>>> {
        let mut runtime = Runtime::new().unwrap();

        let block_path = self.block_path(id);
        let (bytes, code) = runtime.block_on(self.bucket.get_object(&block_path))?;
        if code == NOT_FOUND_CODE {
            Ok(None)
        } else {
            Ok(Some(bytes))
        }
    }

    fn remove_block(&mut self, id: Uuid) -> anyhow::Result<()> {
        let mut runtime = Runtime::new().unwrap();

        let block_path = self.block_path(id);
        runtime.block_on(self.bucket.delete_object(&block_path))?;
        Ok(())
    }

    fn list_blocks(&mut self) -> anyhow::Result<Vec<Uuid>> {
        let mut runtime = Runtime::new().unwrap();

        let blocks_path = join_key!(self.prefix, BLOCK_PREFIX) + SEPARATOR;
        let block_ids = runtime
            .block_on(self.bucket.list(blocks_path.clone(), None))?
            .into_iter()
            .flat_map(|list| list.contents)
            .map(|object| {
                Uuid::parse_str(object.key.trim_start_matches(&blocks_path))
                    .expect("Could not parse UUID.")
            })
            .collect::<Vec<_>>();
        Ok(block_ids)
    }
}
