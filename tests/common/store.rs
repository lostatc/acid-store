#![macro_use]

use rstest_reuse::{self, *};
use tempfile::TempDir;

use acid_store::store::{
    BlockId, BlockKey, BlockType, DataStore, MemoryConfig, MemoryStore, OpenStore,
};
#[cfg(feature = "store-directory")]
use acid_store::store::{DirectoryConfig, DirectoryStore};
#[cfg(feature = "store-rclone")]
use acid_store::store::{RcloneConfig, RcloneStore};
#[cfg(feature = "store-redis")]
use acid_store::store::{RedisConfig, RedisStore};
#[cfg(feature = "store-s3")]
use acid_store::store::{S3Config, S3Credentials, S3Region, S3Store};
#[cfg(feature = "store-sqlite")]
use acid_store::store::{SqliteConfig, SqliteStore};
#[cfg(feature = "store-sftp")]
use {
    acid_store::store::{SftpAuth, SftpConfig, SftpStore},
    std::path::PathBuf,
};

/// Remove all blocks in the given `store`.
fn truncate_store(store: &mut impl DataStore) -> anyhow::Result<()> {
    for block_id in store.list_blocks(BlockType::Data)? {
        store.remove_block(BlockKey::Data(block_id))?;
    }
    for block_id in store.list_blocks(BlockType::Lock)? {
        store.remove_block(BlockKey::Lock(block_id))?;
    }
    for block_id in store.list_blocks(BlockType::Header)? {
        store.remove_block(BlockKey::Header(block_id))?;
    }
    store.remove_block(BlockKey::Super)?;
    store.remove_block(BlockKey::Version)?;

    Ok(())
}

/// A value which is tied to the lifetime of a temporary directory.
struct WithTempDir<T> {
    directory: TempDir,
    value: T,
}

impl<T: DataStore> DataStore for WithTempDir<T> {
    fn write_block(&mut self, key: BlockKey, data: &[u8]) -> anyhow::Result<()> {
        self.value.write_block(key, data)
    }

    fn read_block(&mut self, key: BlockKey) -> anyhow::Result<Option<Vec<u8>>> {
        self.value.read_block(key)
    }

    fn remove_block(&mut self, key: BlockKey) -> anyhow::Result<()> {
        self.value.remove_block(key)
    }

    fn list_blocks(&mut self, kind: BlockType) -> anyhow::Result<Vec<BlockId>> {
        self.value.list_blocks(kind)
    }
}

impl<T: OpenStore> OpenStore for WithTempDir<T> {
    type Store = T::Store;

    fn open(&self) -> acid_store::Result<Self::Store> {
        self.value.open()
    }
}

pub fn memory_config() -> Box<dyn OpenStore<Store = MemoryStore>> {
    Box::new(MemoryConfig::new())
}

pub fn memory_store() -> Box<dyn DataStore> {
    Box::new(memory_config().open().unwrap())
}

#[cfg(feature = "store-directory")]
pub fn directory_config() -> Box<dyn OpenStore<Store = DirectoryStore>> {
    let directory = tempfile::tempdir().unwrap();
    let config = DirectoryConfig {
        path: directory.as_ref().join("store"),
    };
    Box::new(WithTempDir {
        directory,
        value: config,
    })
}

#[cfg(feature = "store-directory")]
pub fn directory_store() -> Box<dyn DataStore> {
    let directory = tempfile::tempdir().unwrap();
    let config = DirectoryConfig {
        path: directory.as_ref().join("store"),
    };
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    Box::new(WithTempDir {
        directory,
        value: store,
    })
}

#[cfg(feature = "store-sqlite")]
pub fn sqlite_config() -> Box<dyn OpenStore<Store = SqliteStore>> {
    let directory = tempfile::tempdir().unwrap();
    let config = SqliteConfig {
        path: directory.as_ref().join("store.db"),
    };
    Box::new(WithTempDir {
        directory,
        value: config,
    })
}

#[cfg(feature = "store-sqlite")]
pub fn sqlite_store() -> Box<dyn DataStore> {
    let directory = tempfile::tempdir().unwrap();
    let config = SqliteConfig {
        path: directory.as_ref().join("store.db"),
    };
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    Box::new(WithTempDir {
        directory,
        value: store,
    })
}

#[cfg(feature = "store-redis")]
pub fn redis_config() -> Box<dyn OpenStore<Store = RedisStore>> {
    let url = dotenv::var("REDIS_URL").unwrap();
    Box::new(RedisConfig::from_url(&url).unwrap())
}

#[cfg(feature = "store-redis")]
pub fn redis_store() -> Box<dyn DataStore> {
    let config = redis_config();
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    Box::new(store)
}

#[cfg(feature = "store-s3")]
pub fn s3_config() -> Box<dyn OpenStore<Store = S3Store>> {
    Box::new(S3Config {
        bucket: dotenv::var("S3_BUCKET").unwrap(),
        region: S3Region::from_name(&dotenv::var("S3_REGION").unwrap()).unwrap(),
        credentials: S3Credentials::Basic {
            access_key: dotenv::var("S3_ACCESS_KEY").unwrap(),
            secret_key: dotenv::var("S3_SECRET_KEY").unwrap(),
        },
        prefix: String::from("test"),
    })
}

#[cfg(feature = "store-s3")]
pub fn s3_store() -> Box<dyn DataStore> {
    let config = s3_config();
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    Box::new(store)
}

#[cfg(feature = "store-sftp")]
pub fn sftp_config() -> Box<dyn OpenStore<Store = SftpStore>> {
    let sftp_server: String = dotenv::var("SFTP_SERVER").unwrap();
    let sftp_path: String = dotenv::var("SFTP_PATH").unwrap();
    let sftp_username: String = dotenv::var("SFTP_USERNAME").unwrap();
    let sftp_password: String = dotenv::var("SFTP_PASSWORD").unwrap();

    Box::new(SftpConfig {
        addr: sftp_server.parse().unwrap(),
        auth: SftpAuth::Password {
            username: sftp_username,
            password: sftp_password,
        },
        path: PathBuf::from(sftp_path),
    })
}
#[cfg(feature = "store-sftp")]
pub fn sftp_store() -> Box<dyn DataStore> {
    let config = sftp_config();
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    Box::new(store)
}

#[cfg(feature = "store-rclone")]
pub fn rclone_config() -> Box<dyn OpenStore<Store = RcloneStore>> {
    Box::new(RcloneConfig {
        config: dotenv::var("RCLONE_REMOTE").unwrap(),
    })
}

#[cfg(feature = "store-rclone")]
pub fn rclone_store() -> Box<dyn DataStore> {
    let config = rclone_config();
    let mut store = config.open().unwrap();
    truncate_store(&mut store).unwrap();
    Box::new(store)
}

/// A parameterized test template which provides a data store config of each type.
///
/// The generates tests are serialized to avoid race conditions with concurrent access to shared
/// resources.
#[template]
#[rstest]
#[case::store_memory(memory_config())]
#[cfg_attr(feature = "store-directory", case::store_directory(directory_config()))]
#[cfg_attr(feature = "store-sqlite", case::store_sqlilte(sqlite_config()))]
#[cfg_attr(feature = "store-redis", case::store_redis(redis_config()))]
#[cfg_attr(feature = "store-s3", case::store_s3(s3_config()))]
#[cfg_attr(feature = "store-sftp", case::store_sftp(sftp_config()))]
#[cfg_attr(feature = "store-rclone", case::store_rclone(rclone_config()))]
pub fn data_configs(#[case] config: Box<dyn OpenStore>) {}

/// A parameterized test template which provides a data store of each type.
///
/// The generates tests are serialized to avoid race conditions with concurrent access to shared
/// resources.
#[template]
#[rstest]
#[case::store_memory(memory_store())]
#[cfg_attr(feature = "store-directory", case::store_directory(directory_store()))]
#[cfg_attr(feature = "store-sqlite", case::store_sqlilte(sqlite_store()))]
#[cfg_attr(feature = "store-redis", case::store_redis(redis_store()))]
#[cfg_attr(feature = "store-s3", case::store_s3(s3_store()))]
#[cfg_attr(feature = "store-sftp", case::store_sftp(sftp_store()))]
#[cfg_attr(feature = "store-rclone", case::store_rclone(rclone_store()))]
pub fn data_stores(#[case] store: Box<dyn DataStore>) {}
