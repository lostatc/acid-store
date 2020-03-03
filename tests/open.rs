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

use matches::assert_matches;
use serial_test::serial;
use tempfile::tempdir;

#[cfg(feature = "store-directory")]
use acid_store::store::DirectoryStore;
#[cfg(feature = "store-sqlite")]
use acid_store::store::SqliteStore;
use acid_store::store::{Open, OpenOption};
#[cfg(feature = "store-redis")]
use {acid_store::store::RedisStore, common::REDIS_INFO};
#[cfg(feature = "store-s3")]
use {acid_store::store::S3Store, common::S3_BUCKET};

mod common;

// Some tests in this module use the `serial_test` crate to force them to run in sequence because
// they access a shared resource. However, that crate doesn't seem to support test functions which
// return a `Result`, so those tests return `()` and unwrap `Result`s instead.

#[test]
#[cfg(feature = "store-directory")]
fn directory_create_new_with_existing_store_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::CREATE_NEW)?;
    let result = DirectoryStore::open(temp_dir.as_ref().join("store"), OpenOption::CREATE_NEW);

    assert_matches!(result, Err(acid_store::Error::AlreadyExists));
    Ok(())
}

#[test]
#[cfg(feature = "store-sqlite")]
fn sqlite_create_new_with_existing_store_errs() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    SqliteStore::open(temp_dir.as_ref().join("store.db"), OpenOption::CREATE_NEW)?;
    let result = SqliteStore::open(temp_dir.as_ref().join("store.db"), OpenOption::CREATE_NEW);

    assert_matches!(result, Err(acid_store::Error::AlreadyExists));
    Ok(())
}

#[test]
#[serial(redis)]
#[cfg(feature = "store-redis")]
fn redis_create_new_with_existing_store_errs() {
    RedisStore::open(
        REDIS_INFO.to_owned(),
        OpenOption::CREATE | OpenOption::TRUNCATE,
    )
    .unwrap();
    let result = RedisStore::open(REDIS_INFO.to_owned(), OpenOption::CREATE_NEW);

    assert_matches!(result, Err(acid_store::Error::AlreadyExists));
}

#[test]
#[serial(s3)]
#[cfg(feature = "store-s3")]
fn s3_create_new_with_existing_store_errs() {
    S3Store::open(
        S3_BUCKET.to_owned(),
        OpenOption::CREATE | OpenOption::TRUNCATE,
    )
    .unwrap();
    let result = S3Store::open(S3_BUCKET.to_owned(), OpenOption::CREATE_NEW);

    assert_matches!(result, Err(acid_store::Error::AlreadyExists));
}
