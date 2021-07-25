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
