/*
 * Copyright 2019 Garrett Powell
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

use common::{new_repository, PASSWORD};
use data_store::repo::{LockStrategy, ObjectRepository};
use data_store::store::MemoryStore;

mod common;

#[test]
fn opening_nonexistent_repo_errs() {
    let repository = ObjectRepository::<String, _>::open_repo(
        MemoryStore::open(),
        Some(PASSWORD),
        LockStrategy::Abort,
    );

    if let Err(data_store::Error::NotFound) = repository {
    } else {
        panic!("An `Error::NotFound` should have been returned.")
    }
}
