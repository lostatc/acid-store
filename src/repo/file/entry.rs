use serde::{Deserialize, Serialize};

use crate::repo::state::ObjectKey;

use super::metadata::FileMetadata;
use super::special::SpecialType;

/// A type of entry in a `FileRepo`.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum EntryType<S> {
    /// A regular file.
    File,

    /// A directory.
    Directory,

    /// A special file.
    Special(S),
}

impl<S: SpecialType> From<S> for EntryType<S> {
    fn from(file: S) -> Self {
        EntryType::Special(file)
    }
}

/// An entry in a [`FileRepo`] which represents a regular file, directory, or special file.
///
/// An entry may or may not have metadata associated with it. When an entry is created by archiving
/// a file in the file system ([`FileRepo::archive`]), it will have the metadata of that file.
/// However, entries can also be created that have no metadata. This allows for extracting files to
/// the file system ([`FileRepo::extract`]) without copying any metadata.
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`FileRepo::archive`]: crate::repo::file::FileRepo::archive
/// [`FileRepo::extract`]: crate::repo::file::FileRepo::extract
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Entry<S, M> {
    /// The type of file this entry represents.
    pub kind: EntryType<S>,

    /// The metadata for the file or `None` if the entry has no metadata.
    pub metadata: Option<M>,
}

impl<S: SpecialType, M: FileMetadata> Entry<S, M> {
    /// Create an `Entry` for a new regular file.
    ///
    /// The created entry will have no metadata.
    pub fn file() -> Self {
        Entry {
            kind: EntryType::File,
            metadata: None,
        }
    }

    /// Create an `Entry` for a new directory.
    ///
    /// The created entry will have no metadata.
    pub fn directory() -> Self {
        Entry {
            kind: EntryType::Directory,
            metadata: None,
        }
    }

    /// Create an `Entry` for a new special `file`.
    ///
    /// The created entry will have no metadata.
    pub fn special(file: S) -> Self {
        Entry {
            kind: EntryType::Special(file),
            metadata: None,
        }
    }

    /// Return whether this entry is a regular file.
    pub fn is_file(&self) -> bool {
        matches!(self.kind, EntryType::File)
    }

    /// Return whether this entry is a directory.
    pub fn is_directory(&self) -> bool {
        matches!(self.kind, EntryType::Directory)
    }

    /// Return whether this entry is a special file.
    pub fn is_special(&self) -> bool {
        matches!(self.kind, EntryType::Special(_))
    }
}

/// A type of entry handle.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum HandleType {
    File(ObjectKey),
    Directory,
    Special,
}

/// A handle for accessing the data associated with each entry.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct EntryHandle {
    pub entry: ObjectKey,
    pub kind: HandleType,
}

impl EntryHandle {
    /// Return the `EntryId` of this entry.
    pub fn id(&self) -> EntryId {
        EntryId(self.entry)
    }
}

/// An ID that uniquely identifies an entry in a [`FileRepo`].
///
/// This value can be used to determine if two paths refer to the same entry. You can get the
/// `EntryId` of an entry using [`FileRepo::entry_id`].
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`FileRepo::entry_id`]: crate::repo::file::FileRepo::entry_id
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntryId(ObjectKey);
