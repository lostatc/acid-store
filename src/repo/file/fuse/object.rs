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

use std::collections::HashMap;
use std::io::Write;
use std::mem;

use crate::repo::{Object, ReadOnlyObject};

/// The currently open objects in a repository.
///
/// Because a repository can only have a single object open for writing at a time, there is not
/// a 1:1 relationship between files which have been opened via `Filesystem::open` and objects
/// which are currently open. Instead, this table stores one object open for writing or a map of
/// objects open for reading. Objects will be opened for reading or writing where possible to
/// improve performance.
///
/// While it would be easier to make file I/O stateless and just open a new `Object` on every
/// call to `Filesystem::read` or `Filesystem::write`, this would degrade performance for two
/// reasons:
///
/// 1. Every call to `Filesystem::write` would require calling `Write::flush` on the `Object`
/// because otherwise that data would be lost when the `Object` is dropped.
/// 2. `Object` and `ReadOnlyObject` would have their internal read buffers cleared on every
/// call to `Filesystem::read`, which would mean that every read would require accessing the
/// data store and potentially fetching bytes which have already been read because of the way
/// chunking works in repositories.
///
/// Making this optimization should improve the performance of back-to-back reads because it
/// will allow for proper buffering, and it should improve the performance of back-to-back
/// writes because data will not need to be flushed after every write.
///
/// There are still certain patterns of usage that will be slower because of the limitations of
/// repositories:
///
/// 1. Any call to `Filesystem::write` will drop any objects which are currently open for
/// reading, thus clearing their read buffers.
/// 2. Any call to `Filesystem::write` on an object that's not currently open for writing will
/// require flushing the currently open `Object` if there is one.
/// 3. Any call to `Filesystem::read` on an object that's not currently open for reading or
/// writing will require flushing the currently open `Object` if there is one.
/// 4. Calling other `Filesystem` methods may require flushing the object currently open for
/// writing or dropping the objects currently open for reading.
#[derive(Debug)]
enum OpenObjects<'a> {
    /// The currently open read-write object and its inode.
    Write { object: Object<'a>, inode: u64 },

    /// A map of inodes to open read-only objects.
    Read(HashMap<u64, ReadOnlyObject<'a>>),
}

/// A table of open objects in a repository.
#[derive(Debug, Default)]
pub struct ObjectTable<'a>(Option<OpenObjects<'a>>);

impl<'a> ObjectTable<'a> {
    /// Return a new `ObjectTable` with no open objects.
    pub fn new() -> Self {
        Self(None)
    }

    /// Get a `ReadOnlyObject` for the file with the given `inode` from the table.
    ///
    /// This returns a reference to an existing object in the table if one exists or inserts a new
    /// object into the table using `default` if not.
    pub fn as_read<'b>(
        &'b mut self,
        inode: u64,
        default: impl FnOnce() -> ReadOnlyObject<'a>,
    ) -> crate::Result<&'b mut ReadOnlyObject<'a>> {
        let object = match &mut self.0 {
            Some(table @ OpenObjects::Write { .. }) => {
                let old_table = mem::replace(table, OpenObjects::Read(HashMap::new()));
                match old_table {
                    OpenObjects::Write {
                        mut object,
                        inode: ino,
                    } => {
                        object.flush()?;
                        let new_object = if ino == inode {
                            object.into()
                        } else {
                            default()
                        };
                        match table {
                            OpenObjects::Read(object_map) => {
                                object_map.entry(inode).or_insert(new_object)
                            }
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                }
            }
            Some(OpenObjects::Read(objects)) => objects.entry(inode).or_insert_with(default),
            table => {
                *table = Some(OpenObjects::Read(HashMap::new()));
                match table {
                    Some(OpenObjects::Read(object_map)) => {
                        object_map.entry(inode).or_insert_with(default)
                    }
                    _ => unreachable!(),
                }
            }
        };

        Ok(object)
    }

    /// Get an `Object` for the file with the given `inode` from the table.
    ///
    /// This returns a reference to an existing object in the table if one exists or inserts a new
    /// object into the table using `default` if not.
    pub fn as_write(
        &mut self,
        inode: u64,
        default: impl FnOnce() -> Object<'a>,
    ) -> crate::Result<&mut Object<'a>> {
        match &mut self.0 {
            Some(OpenObjects::Write { object, inode: ino }) => {
                if *ino != inode {
                    object.flush()?;
                    *object = default();
                }
                Ok(object)
            }
            table => {
                *table = Some(OpenObjects::Write {
                    object: default(),
                    inode,
                });
                match table {
                    Some(OpenObjects::Write { object, .. }) => Ok(object),
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Flush the currently open `Object` if there is one.
    ///
    /// If there is no object open for writing, this returns `Ok`.
    pub fn flush(&mut self) -> crate::Result<()> {
        match &mut self.0 {
            Some(OpenObjects::Write { object, .. }) => Ok(object.flush()?),
            _ => Ok(()),
        }
    }

    /// Close all open objects in the table.
    pub fn close(&mut self) {
        self.0 = None;
    }
}
