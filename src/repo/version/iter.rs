use std::collections::{btree_map, hash_map};
use std::fmt::{Debug, Formatter};
use std::iter::{ExactSizeIterator, FusedIterator};

use super::info::{KeyInfo, Version, VersionInfo};
use crate::repo::state::ObjectKey;
use crate::repo::ContentId;

/// An iterator over the keys in a [`VersionRepo`].
///
/// This value is created by [`VersionRepo::keys`].
///
/// [`VersionRepo`]: crate::repo::version::VersionRepo
/// [`VersionRepo::keys`]: crate::repo::version::VersionRepo::keys
#[derive(Debug, Clone)]
pub struct Keys<'a, K>(pub(super) hash_map::Keys<'a, K, KeyInfo>);

impl<'a, K> Iterator for Keys<'a, K> {
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a, K> FusedIterator for Keys<'a, K> {}

impl<'a, K> ExactSizeIterator for Keys<'a, K> {}

/// An iterator over the versions of a key in a [`VersionRepo`].
///
/// This value is created by [`VersionRepo::versions`].
///
/// [`VersionRepo`]: crate::repo::version::VersionRepo
/// [`VersionRepo::versions`]: crate::repo::version::VersionRepo::versions
pub struct Versions<'a> {
    pub(super) versions: btree_map::IntoIter<u32, VersionInfo>,
    pub(super) id_factory: Box<dyn Fn(ObjectKey) -> ContentId + 'a>,
}

impl<'a> Debug for Versions<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.versions.fmt(f)
    }
}

impl<'a> Iterator for Versions<'a> {
    type Item = Version;

    fn next(&mut self) -> Option<Self::Item> {
        self.versions.next().map(|(id, info)| Version {
            id,
            created: info.created,
            content_id: (self.id_factory)(info.id),
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.versions.size_hint()
    }
}

impl<'a> DoubleEndedIterator for Versions<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.versions.next_back().map(|(id, info)| Version {
            id,
            created: info.created,
            content_id: (self.id_factory)(info.id),
        })
    }
}

impl<'a> FusedIterator for Versions<'a> {}

impl<'a> ExactSizeIterator for Versions<'a> {}
