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

//! A virtual file system which can import and export files to the local OS file system.

pub use relative_path::{RelativePath, RelativePathBuf};

pub use self::entry::{Entry, FileType};
#[cfg(feature = "file-metadata")]
pub use self::metadata::CommonMetadata;
pub use self::metadata::{FileMetadata, NoMetadata};
pub use self::repository::FileRepository;
pub use self::special::{NoSpecialType, SpecialType};
#[cfg(all(unix, feature = "file-metadata"))]
pub use {self::metadata::UnixMetadata, self::special::UnixSpecialType};

mod entry;
mod metadata;
mod repository;
mod special;
