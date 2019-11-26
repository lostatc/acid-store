/*
 * Copyright 2019 Wren Powell
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
use std::result::Result as StdResult;

use diesel::ConnectionError;
use diesel::migration::RunMigrationsError;
use thiserror::Error as DeriveError;

/// The error type for this crate.
#[derive(Debug, DeriveError)]
pub enum Error {
    #[error("{0}")]
    Connection(#[from] ConnectionError),

    #[error("{0}")]
    Migration(#[from] RunMigrationsError),

    #[doc(hidden)]
    #[error("")]
    __NonExhaustive,
}

pub type Result<T> = StdResult<T, Error>;
