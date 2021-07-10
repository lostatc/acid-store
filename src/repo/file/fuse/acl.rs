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

use crate::repo::file::{AccessMode, AccessQualifier, FileMetadata, UnixMetadata};
use std::collections::HashMap;

/// The name of the xattr which stores the ACL entries.
pub const ACL_XATTR_NAME: &str = "system.posix_acl_access";

/// A set of file permissions.
///
/// On platforms that support FUSE, ACL entries are implemented using extended attributes. ACL
/// entries are stored in a single xattr with the name `ACL_XATTR_NAME`. This type can be used to
/// convert between the value of this xattr and the permissions model used by `acid-store`.
pub struct Permissions {
    /// The file mode (st_mode).
    ///
    /// This value is included in this struct because the metadata model used by `acid-store` does
    /// not include the `ACL_USER_OBJ`, `ACL_GROUP_OBJ`, or `ACL_OTHER` entry tags. Instead, these
    /// values are represented by the file mode so that there is no need to synchronize them.
    pub mode: u32,

    /// The access control list for the file.
    ///
    /// This is a map of qualifiers to their associated permissions.
    pub acl: HashMap<AccessQualifier, AccessMode>,
}

// Rather than try to manually parse the byte string used to represent the ACL entries or rely on
// FFI to call libacl, we can just create a temporary file and use on `UnixMetadata` to perform the
// conversion. Because temporary files are created in tmpfs on most platforms, this shouldn't
// create a significant performance penalty.

impl Permissions {
    /// Update these permissions from the raw bytes of the ACL xattr.
    pub fn update_attr(&mut self, attr: &[u8]) -> crate::Result<()> {
        let temp_file = tempfile::NamedTempFile::new()?;

        let mut metadata = UnixMetadata::from_file(temp_file.path())?;
        metadata.mode = self.mode;
        metadata.acl = self.acl.clone();
        metadata
            .attributes
            .insert(ACL_XATTR_NAME.to_owned(), attr.to_vec());

        metadata.write_metadata(temp_file.path())?;
        let UnixMetadata { mode, acl, .. } = UnixMetadata::from_file(temp_file.path())?;
        self.mode = mode;
        self.acl = acl;

        Ok(())
    }

    /// Generate the raw bytes of the ACL xattr from this `Permissions`.
    pub fn to_attr(&self) -> crate::Result<Vec<u8>> {
        let temp_file = tempfile::NamedTempFile::new()?;

        let mut metadata = UnixMetadata::from_file(temp_file.path())?;
        metadata.mode = self.mode;
        metadata.acl = self.acl.clone();
        metadata.write_metadata(temp_file.path())?;
        let mut metadata = UnixMetadata::from_file(temp_file.path())?;

        Ok(metadata
            .attributes
            .remove(ACL_XATTR_NAME)
            .unwrap_or_else(Vec::new))
    }
}

impl From<UnixMetadata> for Permissions {
    fn from(metadata: UnixMetadata) -> Self {
        Permissions {
            mode: metadata.mode,
            acl: metadata.acl,
        }
    }
}
