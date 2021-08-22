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

#![macro_use]

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

macro_rules! uuid_type {
    {
        $(#[$meta:meta])*
        $name:ident
    } => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(uuid::Uuid);

        impl $name {
            /// Construct a new instance which wraps a `uuid`.
            pub const fn new(uuid: uuid::Uuid) -> Self {
                $name(uuid)
            }
        }

        impl AsRef<uuid::Uuid> for $name {
            fn as_ref(&self) -> &uuid::Uuid {
                &self.0
            }
        }

        impl From<uuid::Uuid> for $name {
            fn from(uuid: uuid::Uuid) -> Self {
                Self(uuid)
            }
        }

        impl From<$name> for uuid::Uuid {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

/// A table for allocating `u64` values.
#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct IdTable {
    /// The highest used ID value (the high water mark).
    highest: u64,

    /// A set of unused ID values below the high water mark.
    unused: HashSet<u64>,
}

impl IdTable {
    /// Return a new empty `IdTable`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the next unused ID from the table.
    pub fn next(&mut self) -> u64 {
        match self.unused.iter().next().copied() {
            Some(id) => {
                self.unused.remove(&id);
                id
            }
            None => {
                self.highest += 1;
                self.highest
            }
        }
    }

    /// Return whether the given `id` is in the table.
    pub fn contains(&self, id: u64) -> bool {
        id <= self.highest && !self.unused.contains(&id)
    }

    /// Return the given `id` back to the table.
    ///
    /// This returns `true` if the value was returned or `false` if it was unused.
    pub fn recycle(&mut self, id: u64) -> bool {
        if !self.contains(id) {
            return false;
        }
        self.unused.insert(id);
        true
    }
}

macro_rules! id_table {
    {
        $(#[$id_meta:meta])*
        $id_name:ident
        $(#[$table_meta:meta])*
        $table_name:ident
    } => {
        $(#[$id_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $id_name(u64);

        $(#[$table_meta])*
        #[derive(Debug, PartialEq, Eq, Clone, Default, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $table_name(crate::id::IdTable);

        impl $table_name {
            /// Return a new empty instance.
            pub fn new() -> Self {
                Self(crate::id::IdTable::new())
            }

            /// Return the next unused ID from the table.
            pub fn next(&mut self) -> $id_name {
                $id_name(self.0.next())
            }

            /// Return the given `id` back to the table.
            ///
            /// This returns `true` if the value was returned or `false` if it was unused.
            pub fn recycle(&mut self, id: $id_name) -> bool {
                self.0.recycle(id.0)
            }
        }
    }
}
