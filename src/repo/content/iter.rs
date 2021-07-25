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

use std::collections::hash_map;
use std::iter::{ExactSizeIterator, FusedIterator};

use crate::repo::state::ObjectKey;

/// An iterator over the hashes of objects in a [`ContentRepo`].
///
/// This value is created by [`ContentRepo::list`].
///
/// [`ContentRepo`]: crate::repo::content::ContentRepo
/// [`ContentRepo::list`]: crate::repo::content::ContentRepo::list
#[derive(Debug, Clone)]
pub struct List<'a>(pub(super) hash_map::Keys<'a, Vec<u8>, ObjectKey>);

impl<'a> Iterator for List<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|hash| hash.as_slice())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a> FusedIterator for List<'a> {}

impl<'a> ExactSizeIterator for List<'a> {}
