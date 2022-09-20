#![cfg(feature = "store-s3")]

use std::env;

use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use uuid::{uuid, Uuid};

use super::data_store::{BlockId, BlockKey, BlockType, DataStore};
use super::open_store::OpenStore;

/// The separator to use in S3 object keys.
const SEPARATOR: &str = "/";

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

// The keys of objects in the data store.
const STORE_KEY: &str = "store";
const DATA_KEY: &str = "data";
const LOCKS_KEY: &str = "lock";
const HEADERS_KEY: &str = "header";
const SUPER_KEY: &str = "super";
const REPO_VERSION_KEY: &str = "version";
const STORE_VERSION_KEY: &str = "version";

/// A UUID which acts as the version ID of the store format.
const CURRENT_VERSION: Uuid = uuid!("f0511da2-f90d-11eb-be71-13f36b8156e4");

/// The HTTP status code for an object which does not exist.
const NOT_FOUND_CODE: u16 = 404;

/// The environment variable for the AWS access key.
const ACCESS_KEY_ENV: &str = "AWS_ACCESS_KEY_ID";

/// The environment variable for the AWS secrete key.
const SECRET_KEY_ENV: &str = "AWS_SECRET_ACCESS_KEY";

/// The environment variable for the AWS session token.
const SESSION_TOKEN_ENV: &str = "AWS_SESSION_TOKEN";

/// An AWS region.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-s3")))]
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
#[cfg_attr(docsrs, doc(cfg(feature = "store-s3")))]
pub enum S3Credentials {
    /// Anonymous credentials for accessing public objects.
    Anonymous,

    /// Basic credentials.
    Basic {
        /// Access key ID.
        access_key: String,

        /// Secret access key.
        secret_key: String,
    },

    /// Session credentials.
    Session {
        /// Access key ID.
        access_key: String,

        /// Secret access key.
        secret_key: String,

        /// Session token.
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

/// The configuration for opening an [`S3Store`].
///
/// [`S3Store`]: crate::store::S3Store
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-s3")))]
pub struct S3Config {
    /// The name of the S3 bucket.
    pub bucket: String,

    /// The AWS region to connect to.
    pub region: S3Region,

    /// The credentials to connect with.
    pub credentials: S3Credentials,

    /// The prefix to prepend to keys in the store.
    ///
    /// While keys in S3 are a flat namespace, you can think of this like the directory of the
    /// bucket to create the store in. To create the store in the bucket root, use an empty string.
    pub prefix: String,
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
                    expiration: None,
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
                    expiration: None,
                },
            },
        )
        .unwrap()
    }
}

impl OpenStore for S3Config {
    type Store = S3Store;

    fn open(&self) -> crate::Result<Self::Store> {
        let bucket = self.clone().into_bucket();
        let prefix = self.prefix.trim_end_matches(SEPARATOR).to_owned();
        let version_key = join_key!(prefix, STORE_VERSION_KEY);

        match bucket.get_object(&version_key) {
            Ok(response) if response.status_code() == NOT_FOUND_CODE => {
                bucket
                    .put_object(&version_key, CURRENT_VERSION.as_bytes())
                    .map_err(|error| crate::Error::Store(anyhow::Error::from(error)))?;
            }
            Ok(response) => {
                let version = Uuid::from_slice(response.bytes())
                    .map_err(|_| crate::Error::UnsupportedStore)?;
                if version != CURRENT_VERSION {
                    return Err(crate::Error::UnsupportedStore);
                }
            }
            Err(error) => return Err(crate::Error::Store(anyhow::Error::from(error))),
        };

        Ok(S3Store { bucket, prefix })
    }
}

/// A `DataStore` which stores data in an Amazon S3 bucket.
///
/// You can use [`S3Config`] to open a data store of this type.
///
/// [`S3Config`]: crate::store::S3Config
#[derive(Debug)]
#[cfg_attr(docsrs, doc(cfg(feature = "store-s3")))]
pub struct S3Store {
    bucket: Bucket,
    prefix: String,
}

impl S3Store {
    /// Return the key of the block with the given `id`.
    fn block_path(&self, key: BlockKey) -> String {
        match key {
            BlockKey::Data(id) => {
                join_key!(
                    self.prefix,
                    STORE_KEY,
                    DATA_KEY,
                    id.as_ref().as_hyphenated().to_string()
                )
            }
            BlockKey::Lock(id) => join_key!(
                self.prefix,
                STORE_KEY,
                LOCKS_KEY,
                id.as_ref().as_hyphenated().to_string()
            ),
            BlockKey::Header(id) => join_key!(
                self.prefix,
                STORE_KEY,
                HEADERS_KEY,
                id.as_ref().as_hyphenated().to_string()
            ),
            BlockKey::Super => join_key!(self.prefix, STORE_KEY, SUPER_KEY),
            BlockKey::Version => join_key!(self.prefix, STORE_KEY, REPO_VERSION_KEY),
        }
    }
}

impl DataStore for S3Store {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        let block_path = self.block_path(key);
        self.bucket.put_object(&block_path, data)?;
        Ok(())
    }

    fn read_block(&mut self, key: BlockKey) -> anyhow::Result<Option<Vec<u8>>> {
        let block_path = self.block_path(key);
        let response = self.bucket.get_object(&block_path)?;
        if response.status_code() == NOT_FOUND_CODE {
            Ok(None)
        } else {
            Ok(Some(response.bytes().into()))
        }
    }

    fn remove_block(&mut self, key: BlockKey) -> anyhow::Result<()> {
        let block_path = self.block_path(key);
        self.bucket.delete_object(&block_path)?;
        Ok(())
    }

    fn list_blocks(&mut self, kind: BlockType) -> anyhow::Result<Vec<BlockId>> {
        let blocks_key = match kind {
            BlockType::Data => join_key!(self.prefix, STORE_KEY, DATA_KEY) + SEPARATOR,
            BlockType::Lock => join_key!(self.prefix, STORE_KEY, LOCKS_KEY) + SEPARATOR,
            BlockType::Header => join_key!(self.prefix, STORE_KEY, HEADERS_KEY) + SEPARATOR,
        };

        let block_ids = self
            .bucket
            .list(blocks_key.clone(), None)?
            .into_iter()
            .flat_map(|list| list.contents)
            .map(|object| {
                Uuid::parse_str(object.key.trim_start_matches(&blocks_key)).map(|id| id.into())
            })
            .collect::<Result<Vec<BlockId>, _>>()?;
        Ok(block_ids)
    }
}
