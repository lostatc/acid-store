/*
 * Copyright 2019-2021 Wren Powell
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

pub use self::chunking::Chunking;
pub use self::commit::Commit;
pub use self::compression::Compression;
pub use self::config::RepoConfig;
pub use self::encryption::{Encryption, ResourceLimit};
pub use self::handle::ContentId;
pub use self::id_table::{IdTable, UniqueId};
pub use self::key::Key;
pub use self::metadata::{peek_info, RepoInfo};
pub use self::object::{Object, ReadOnlyObject};
pub use self::open_options::{OpenMode, OpenOptions, DEFAULT_INSTANCE};
pub use self::open_repo::{OpenRepo, SwitchInstance};
pub use self::packing::Packing;
pub use self::repository::KeyRepo;
pub use self::savepoint::{Restore, RestoreSavepoint, Savepoint};

mod chunk_store;
mod chunking;
mod commit;
mod compression;
mod config;
mod encryption;
mod handle;
mod id_table;
mod key;
mod lock;
mod metadata;
mod object;
mod object_io;
mod open_options;
mod open_repo;
mod packing;
mod repository;
mod savepoint;
mod state;
