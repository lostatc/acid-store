use std::collections::hash_map;
use std::iter::{ExactSizeIterator, FusedIterator};

use crate::repo::state::ObjectKey;

/// An iterator over the hashes of objects in a [`ContentRepo`].
///
/// This value is created by [`ContentRepo::hashes`].
///
/// [`ContentRepo`]: crate::repo::content::ContentRepo
/// [`ContentRepo::hashes`]: crate::repo::content::ContentRepo::hashes
#[derive(Debug, Clone)]
pub struct Hashes<'a>(pub(super) hash_map::Keys<'a, Vec<u8>, ObjectKey>);

impl<'a> Iterator for Hashes<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|hash| hash.as_slice())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a> FusedIterator for Hashes<'a> {}

impl<'a> ExactSizeIterator for Hashes<'a> {}
