use std::iter::{ExactSizeIterator, FusedIterator};

use super::info::{ObjectKey, RepoKey};
use crate::repo::{key, InstanceId, RepoId};

/// An iterator over the keys in a [`StateRepo`].
///
/// This value is created by [`StateRepo::keys`].
///
/// [`StateRepo`]: crate::repo::state::StateRepo
/// [`StateRepo::keys`]: crate::repo::state::StateRepo::keys
#[derive(Debug, Clone)]
pub struct Keys<'a> {
    pub(super) repo_id: RepoId,
    pub(super) instance_id: InstanceId,
    pub(super) inner: key::Keys<'a, RepoKey>,
}

impl<'a> Iterator for Keys<'a> {
    type Item = ObjectKey;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                None => return None,
                Some(RepoKey::Object(key_id)) => {
                    return Some(ObjectKey {
                        repo_id: self.repo_id,
                        instance_id: self.instance_id,
                        key_id: *key_id,
                    })
                }
                _ => continue,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a> FusedIterator for Keys<'a> {}

impl<'a> ExactSizeIterator for Keys<'a> {}
