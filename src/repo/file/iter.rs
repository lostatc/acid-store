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

use std::iter::{ExactSizeIterator, FusedIterator};

use relative_path::RelativePathBuf;

use super::entry::EntryHandle;
use super::path_tree;

/// An iterator over the children of an entry in a [`FileRepo`].
///
/// This value is created by [`FileRepo::children`].
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`FileRepo::children`]: crate::repo::file::FileRepo::children
#[derive(Debug, Clone)]
pub struct Children<'a>(pub(super) path_tree::Children<'a, EntryHandle>);

impl<'a> Iterator for Children<'a> {
    type Item = RelativePathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(path, _)| path)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<'a> FusedIterator for Children<'a> {}

impl<'a> ExactSizeIterator for Children<'a> {}

/// An iterator over the descendants of an entry in a [`FileRepo`].
///
/// This value is created by [`FileRepo::descendants`].
///
/// [`FileRepo`]: crate::repo::file::FileRepo
/// [`FileRepo::descendants`]: crate::repo::file::FileRepo::descendants
#[derive(Debug)]
pub struct Descendants<'a>(pub(super) path_tree::Descendants<'a, EntryHandle>);

impl<'a> Iterator for Descendants<'a> {
    type Item = RelativePathBuf;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(path, _)| path)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
