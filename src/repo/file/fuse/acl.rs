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

use crate::repo::file::{Acl, FileMetadata, FileMode, UnixMetadata};

/// The name of the xattr which stores the ACL access entries.
pub const ACCESS_ACL_XATTR: &str = "system.posix_acl_access";

/// The name of the xattr which stores the ACL default entries.
pub const DEFAULT_ACL_XATTR: &str = "system.posix_acl_default";

/// A set of file permissions.
///
/// On platforms that support FUSE, ACL entries are implemented using extended attributes. ACL
/// entries are stored the xattrs with the names `ACCESS_ACL_XATTR` and `DEFAULT_ACL_XATTR`. This
/// type can be used to convert between the value of these xattrs and the permissions model used by
/// `acid-store`.
pub struct Permissions {
    /// The file mode (st_mode).
    pub mode: u32,

    /// The access control lists for the file.
    pub acl: Acl,
}

// Rather than try to manually parse the byte string used to represent the ACL entries or rely on
// FFI to call libacl, we can just create a temporary file and use on `UnixMetadata` to perform the
// conversion. Because temporary files are created in tmpfs on most platforms, this shouldn't
// create a significant performance penalty.

impl Permissions {
    /// Update these permissions from the raw bytes of the given xattr.
    ///
    /// This accepts the `name` and `value` of the xattr.
    pub fn update_attr(&mut self, name: &str, value: &[u8]) -> crate::Result<()> {
        // We use a directory because default ACLs can only be set on a directory.
        let temp_file = tempfile::tempdir()?;

        let mut metadata = UnixMetadata::from_file(temp_file.path())?.unwrap();
        metadata.mode = FileMode::from_bits_truncate(self.mode);
        metadata.acl = Acl::new();
        metadata.attributes.insert(name.to_owned(), value.to_vec());

        metadata.write_metadata(temp_file.path())?;
        let UnixMetadata { mode, acl, .. } = UnixMetadata::from_file(temp_file.path())?.unwrap();

        // We want to replace the rwx bits and keep the rest of the bits unchanged.
        self.mode = (self.mode & !0o777) | (mode.bits() & 0o777);
        self.acl = acl;

        Ok(())
    }

    /// Generate the raw bytes of the ACL xattr from this `Permissions`.
    ///
    /// This accepts the `name` of the xattr.
    pub fn to_attr(&self, name: &str) -> crate::Result<Vec<u8>> {
        // We use a directory because default ACLs can only be set on a directory.
        let temp_file = tempfile::tempdir()?;

        let mut metadata = UnixMetadata::from_file(temp_file.path())?.unwrap();
        metadata.mode = FileMode::from_bits_truncate(self.mode & 0o777);
        metadata.acl = self.acl.clone();
        metadata.write_metadata(temp_file.path())?;
        let mut metadata = UnixMetadata::from_file(temp_file.path())?.unwrap();

        Ok(metadata.attributes.remove(name).unwrap_or_else(Vec::new))
    }
}

impl From<UnixMetadata> for Permissions {
    fn from(metadata: UnixMetadata) -> Self {
        Permissions {
            mode: metadata.mode.bits(),
            acl: metadata.acl,
        }
    }
}
