use rand::prelude::*;
use rstest::*;

use acid_store::repo::{
    key::KeyRepo, InstanceId, Object, OpenMode, OpenOptions, OpenRepo, RepoConfig, DEFAULT_INSTANCE,
};
use acid_store::store::MemoryConfig;
use rand::distributions::{Alphanumeric, DistString};

const KEY_LEN: usize = 16;

const PASSWORD_LEN: usize = 16;

/// A test helper which encapsulates a repository and an object.
pub struct RepoObject {
    pub repo: KeyRepo<String>,
    pub object: Object,
    pub key: String,
}

impl RepoObject {
    pub fn new(config: RepoConfig) -> anyhow::Result<Self> {
        let mut repo: KeyRepo<String> = create_repo(config)?;
        let mut rng = SmallRng::from_entropy();
        let key = Alphanumeric.sample_string(&mut rng, KEY_LEN);
        let object = repo.insert(key.clone());
        Ok(RepoObject { repo, object, key })
    }
}

pub type BoxLockHandler = Box<dyn Fn(&[u8]) -> bool>;

/// A test helper for opening multiple repositories backed by the same data store.
pub struct RepoStore {
    pub store: MemoryConfig,
    pub config: RepoConfig,
    pub password: String,
    pub instance: InstanceId,
    pub context: Vec<u8>,
    pub handler: BoxLockHandler,
}

impl RepoStore {
    pub fn new(config: RepoConfig) -> Self {
        let mut rng = SmallRng::from_entropy();
        let password = Alphanumeric.sample_string(&mut rng, PASSWORD_LEN);
        RepoStore {
            store: MemoryConfig::new(),
            config,
            password,
            instance: DEFAULT_INSTANCE,
            context: Vec::new(),
            handler: Box::new(|_| false),
        }
    }

    /// Create a new repository.
    pub fn create<R: OpenRepo>(&self) -> acid_store::Result<R> {
        OpenOptions::new()
            .config(self.config.clone())
            .password(self.password.as_bytes())
            .instance(self.instance)
            .locking(&self.context, |context| (self.handler)(context))
            .mode(OpenMode::CreateNew)
            .open(&self.store)
    }

    /// Open an existing repository.
    pub fn open<R: OpenRepo>(&self) -> acid_store::Result<R> {
        OpenOptions::new()
            .config(self.config.clone())
            .password(self.password.as_bytes())
            .instance(self.instance)
            .locking(&self.context, |context| (self.handler)(context))
            .mode(OpenMode::Open)
            .open(&self.store)
    }
}

pub fn create_repo<R: OpenRepo>(config: RepoConfig) -> anyhow::Result<R> {
    let store_config = MemoryConfig::new();
    Ok(OpenOptions::new()
        .config(config)
        .password(b"Password")
        .mode(OpenMode::CreateNew)
        .open(&store_config)?)
}

/// A test fixture which provides a new empty repository.
#[fixture]
pub fn repo<R: OpenRepo>() -> R {
    create_repo(RepoConfig::default()).unwrap()
}

/// A test fixture which provides a new empty `RepoObject`.
#[fixture]
pub fn repo_object() -> RepoObject {
    RepoObject::new(RepoConfig::default()).unwrap()
}

/// A test fixture which provides a new empty `RepoStore`.
#[fixture]
pub fn repo_store() -> RepoStore {
    RepoStore::new(RepoConfig::default())
}
