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

use std::cmp::min;
use std::collections::HashMap;
use std::mem::size_of;

use uuid::Uuid;

use crate::store::DataStore;

use super::state::{Pack, PackIndex, RepoState};

/// A method for packing data in a repository.
///
/// By default, repositories do not attempt to hide the size of chunks produced by the chunker.
/// Even when using fixed-size chunking, chunks which are smaller than the configured chunk size can
/// still be produced. This is a form of metadata leakage which may be undesirable in some cases.
///
/// To fix this problem, it is possible to configure the repository to pack data into fixed-size
/// blocks before writing it to the data store. This hides the size of chunks produced by the
/// chunker at the cost of performance.
///
/// Choosing `Packing::Fixed` provides no additional security if encryption is disabled. If
/// encryption is not needed, you should use `Packing::None`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Packing {
    /// Do not pack data into fixed-size blocks.
    ///
    /// This typically provides better performance than `Packing::Fixed`.
    None,

    /// Pack data into fixed-size blocks.
    ///
    /// This accepts the size in bytes of the blocks to produce.
    ///
    /// This typically results in worse performance than `Packing::None`. This can also cause some
    /// storage overhead, especially when using a large block size.
    Fixed(u32),
}
