use std::collections::hash_map;
use std::hash::Hash;
use std::iter::{ExactSizeIterator, FusedIterator};
use std::sync::{Arc, RwLock};

use serde::de::DeserializeOwned;
use serde::Serialize;

use super::handle::ObjectHandle;

/// A type which can be used as a key in a [`KeyRepo`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
pub trait Key: Eq + Hash + Clone + Serialize + DeserializeOwned {}

impl<T> Key for T where T: Eq + Hash + Clone + Serialize + DeserializeOwned {}

/// An iterator over the keys in a [`KeyRepo`].
///
/// This value is created by [`KeyRepo::keys`].
///
/// [`KeyRepo`]: crate::repo::key::KeyRepo
/// [`KeyRepo::keys`]: crate::repo::key::KeyRepo::keys
#[derive(Debug, Clone)]
pub struct Keys<'a, K>(pub(super) hash_map::Keys<'a, K, Arc<RwLock<ObjectHandle>>>);

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
