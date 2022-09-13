use std::collections::hash_map;
use std::iter::{ExactSizeIterator, FusedIterator};

use crate::repo::state::ObjectKey;

/// An iterator over the keys in a [`ValueRepo`].
///
/// This value is created by [`ValueRepo::keys`].
///
/// [`ValueRepo`]: crate::repo::value::ValueRepo
/// [`ValueRepo::keys`]: crate::repo::value::ValueRepo::keys
#[derive(Debug, Clone)]
pub struct Keys<'a, K>(pub(super) hash_map::Keys<'a, K, ObjectKey>);

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
