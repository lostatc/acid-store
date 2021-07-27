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

use std::collections::{hash_map, HashMap};
use std::fmt::{Debug, Formatter};
use std::iter::{self, ExactSizeIterator, FusedIterator};

use relative_path::{RelativePath, RelativePathBuf};
use serde::{Deserialize, Serialize};

use super::iter::WalkPredicate;

/// Recursively iterate through the tree of nodes.
fn descendants<'a, V>(
    parent: impl AsRef<RelativePath> + 'a,
    children: &'a HashMap<String, PathNode<V>>,
) -> Box<dyn Iterator<Item = (RelativePathBuf, &'a V)> + 'a> {
    Box::new(children.iter().flat_map(move |(name, node)| {
        iter::once((parent.as_ref().join(name), &node.value))
            .chain(descendants(parent.as_ref().join(name), &node.children))
    }))
}

/// Recursively iterate through the tree of nodes and remove them.
fn drain_nodes<'a, V: 'a>(
    parent: impl AsRef<RelativePath> + 'a,
    children: HashMap<String, PathNode<V>>,
) -> Box<dyn Iterator<Item = (RelativePathBuf, V)> + 'a> {
    Box::new(children.into_iter().flat_map(move |(name, node)| {
        let PathNode { children, value } = node;
        iter::once((parent.as_ref().join(&name), value))
            .chain(drain_nodes(parent.as_ref().join(&name), children))
    }))
}

/// An iterator over the children of a path in a `PathTree`.
#[derive(Debug, Clone)]
pub struct Children<'a, V> {
    parent: RelativePathBuf,
    children: hash_map::Iter<'a, String, PathNode<V>>,
}

impl<'a, V> Iterator for Children<'a, V> {
    type Item = (RelativePathBuf, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.children
            .next()
            .map(|(name, node)| (self.parent.join(name), &node.value))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.children.size_hint()
    }
}

impl<'a, V> FusedIterator for Children<'a, V> {}

impl<'a, V> ExactSizeIterator for Children<'a, V> {}

/// An iterator over the children of a path in a `PathTree`.
pub struct Descendants<'a, V> {
    parent: RelativePathBuf,
    inner: Box<dyn Iterator<Item = (RelativePathBuf, &'a V)> + 'a>,
}

impl<'a, V> Iterator for Descendants<'a, V> {
    type Item = (RelativePathBuf, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub struct WalkEntry<'a, V> {
    pub path: RelativePathBuf,
    pub value: &'a V,
    pub depth: usize,
}

/// Recursively walk through the tree of descendants of `parent`.
fn walk<V>(
    parent: RelativePathBuf,
    node: &PathNode<V>,
    depth: usize,
    visitor: &mut impl FnMut(WalkEntry<V>) -> WalkPredicate,
) -> WalkPredicate {
    let walk_entry = WalkEntry {
        path: parent.clone(),
        value: &node.value,
        depth,
    };

    let predicate: WalkPredicate = visitor(walk_entry);

    if let WalkPredicate::Continue | WalkPredicate::SkipSiblings = predicate {
        for (child_name, child_node) in &node.children {
            match walk(parent.join(child_name), child_node, depth + 1, visitor) {
                WalkPredicate::SkipSiblings => return predicate,
                WalkPredicate::Stop => return WalkPredicate::Stop,
                _ => {}
            }
        }
    }

    predicate
}

impl<'a, V> Debug for Descendants<'a, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Walk")
            .field("parent", &self.parent)
            .finish()
    }
}

/// A node in a `PathTree`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PathNode<V> {
    /// The file's children.
    children: HashMap<String, PathNode<V>>,

    /// The associated value.
    value: V,
}

impl<V> PathNode<V> {
    fn new(value: V) -> Self {
        PathNode {
            children: HashMap::new(),
            value,
        }
    }
}

/// A tree that associates file paths with values of type `V`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathTree<V> {
    nodes: HashMap<String, PathNode<V>>,
}

impl<V> Default for PathTree<V> {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }
}

impl<V> PathTree<V> {
    /// Return a new empty `PathTree`.
    pub fn new() -> Self {
        PathTree {
            nodes: HashMap::new(),
        }
    }

    /// Returns whether the given `path` is in the tree.
    pub fn contains(&self, path: impl AsRef<RelativePath>) -> bool {
        self.get(path).is_some()
    }

    /// Return the value associated with `path`.
    ///
    /// This returns `None` if `path` is not in the tree or does not have a value associated with
    /// it.
    pub fn get(&self, path: impl AsRef<RelativePath>) -> Option<&V> {
        let mut current_nodes = &self.nodes;
        let mut current_value = None;

        for segment in path.as_ref().iter() {
            let node = current_nodes.get(segment)?;
            current_nodes = &node.children;
            current_value = Some(&node.value);
        }

        current_value
    }

    /// Return the value associated with `path`.
    ///
    /// This returns `None` if `path` is not in the tree or does not have a value associated with
    /// it.
    pub fn get_mut(&mut self, path: impl AsRef<RelativePath>) -> Option<&mut V> {
        let mut current_nodes = &mut self.nodes;
        let mut current_value = None;

        for segment in path.as_ref().iter() {
            let node = current_nodes.get_mut(segment)?;
            current_nodes = &mut node.children;
            current_value = Some(&mut node.value);
        }

        current_value
    }

    /// Insert the given `path` and `value` into the tree.
    ///
    /// This returns the value of the existing path if it already existed or `None` if it did not.
    ///
    /// # Panics
    /// - The parent path does not exist.
    pub fn insert(&mut self, path: impl AsRef<RelativePath>, value: V) -> Option<V> {
        let mut current_nodes = &mut self.nodes;
        let mut segments = path.as_ref().iter();
        let mut segment = segments.next()?;

        for next_segment in segments {
            let node = match current_nodes.get_mut(segment) {
                Some(node) => node,
                None => panic!("The parent path does not exist."),
            };

            current_nodes = &mut node.children;

            segment = next_segment;
        }

        current_nodes
            .insert(segment.to_string(), PathNode::new(value))
            .map(|node| node.value)
    }

    /// Remove the given `path` and its descendants from the tree .
    ///
    /// If the path is in the tree, this returns its value. Otherwise, this returns `None`.
    pub fn remove(&mut self, path: impl AsRef<RelativePath>) -> Option<V> {
        let mut current_nodes = &mut self.nodes;
        let mut segments = path.as_ref().iter();
        let mut segment = segments.next()?;

        for next_segment in segments {
            let node = current_nodes.get_mut(segment)?;
            current_nodes = &mut node.children;
            segment = next_segment;
        }

        Some(current_nodes.remove(segment)?.value)
    }

    /// Return an iterator of the children of `path` and their values.
    ///
    /// If the path is not in the tree, this returns `None`.
    ///
    /// The returned iterator does not include the parent `path`.
    pub fn children<'a>(&'a self, path: impl AsRef<RelativePath> + 'a) -> Option<Children<'a, V>> {
        let mut current_nodes = &self.nodes;

        for segment in path.as_ref().iter() {
            current_nodes = &current_nodes.get(segment)?.children;
        }

        Some(Children {
            parent: path.as_ref().to_owned(),
            children: current_nodes.iter(),
        })
    }

    /// Return an iterator of the descendants of `path` and their values.
    ///
    /// If the path is not in the tree, this returns `None`.
    ///
    /// The returned iterator does not include the parent `path`. Descendants are returned in
    /// depth-first order.
    pub fn descendants<'a>(
        &'a self,
        path: impl AsRef<RelativePath> + 'a,
    ) -> Option<Descendants<'a, V>> {
        let mut current_nodes = &self.nodes;

        for segment in path.as_ref().iter() {
            current_nodes = &current_nodes.get(segment)?.children;
        }

        Some(Descendants {
            parent: path.as_ref().to_owned(),
            inner: descendants(path, current_nodes),
        })
    }

    /// Walk through the descendants of `parent`.
    ///
    /// If the `parent` is not in this tree, this returns `None`. If the visitor exits early via
    /// `WalkPredicate::Stop`, this returns `Some(true)`. Otherwise, this returns `Some(false)`.
    ///
    /// Paths are iterated over in depth-first order, so a path always comes before its children.
    pub fn walk(
        &self,
        parent: impl AsRef<RelativePath>,
        mut visitor: impl FnMut(WalkEntry<V>) -> WalkPredicate,
    ) -> Option<bool> {
        let mut current_nodes = &self.nodes;

        for segment in parent.as_ref().iter() {
            current_nodes = match current_nodes.get(segment) {
                Some(node) => &node.children,
                None => return None,
            };
        }

        for (child_name, child_node) in current_nodes {
            match walk(
                parent.as_ref().join(child_name),
                child_node,
                1,
                &mut visitor,
            ) {
                WalkPredicate::Stop => return Some(true),
                WalkPredicate::SkipSiblings => return Some(false),
                _ => continue,
            }
        }

        Some(false)
    }

    /// Drain the tree of the descendants of `path` and their values.
    ///
    /// If the path is not in the tree, this returns `None`.
    ///
    /// The returned iterator includes the parent `path`. Descendants are returned in
    /// depth-first order.
    pub fn drain<'a>(
        &'a mut self,
        path: impl AsRef<RelativePath> + 'a,
    ) -> Option<Box<dyn Iterator<Item = (RelativePathBuf, V)> + 'a>> {
        let mut current_nodes = &mut self.nodes;
        let mut segments = path.as_ref().iter();
        let mut segment = segments.next()?;

        for next_segment in segments {
            let node = current_nodes.get_mut(segment)?;
            current_nodes = &mut node.children;
            segment = next_segment;
        }

        let PathNode { value, children } = current_nodes.remove(segment)?;
        Some(Box::new(
            iter::once((path.as_ref().to_owned(), value)).chain(drain_nodes(path, children)),
        ))
    }

    /// Remove all paths and values from the tree.
    pub fn clear(&mut self) {
        self.nodes.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use maplit::hashset;
    use relative_path::RelativePathBuf;

    use crate::repo::file::path_tree::PathTree;
    use crate::repo::file::WalkPredicate;

    #[test]
    fn tree_contains_path() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);

        assert!(tree.contains("a"));
        assert!(tree.contains("a/b"));
        assert!(!tree.contains("a/c"));
    }

    #[test]
    fn insert_paths_and_get() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.insert("a/b/c", 3);
        tree.insert("a/b/d", 4);

        assert_eq!(tree.get("a"), Some(&1));
        assert_eq!(tree.get("a/b"), Some(&2));
        assert_eq!(tree.get("a/b/c"), Some(&3));
        assert_eq!(tree.get("a/b/d"), Some(&4));
    }

    #[test]
    #[should_panic]
    fn inserting_with_missing_parent_panics() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b/c", 2);
    }

    #[test]
    fn removed_paths_do_not_exist() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.insert("a/c", 3);
        tree.remove("a/b");

        assert_eq!(tree.get("a"), Some(&1));
        assert_eq!(tree.get("a/b"), None);
        assert_eq!(tree.get("a/c"), Some(&3));
    }

    #[test]
    fn removing_parent_removes_children() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.remove("a");

        assert_eq!(tree.get("a"), None);
        assert_eq!(tree.get("a/b"), None);
    }

    #[test]
    fn list_children() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.insert("a/c", 3);
        tree.insert("a/b/d", 3);

        let expected = hashset![
            (RelativePathBuf::from("a/b"), &2),
            (RelativePathBuf::from("a/c"), &3),
        ];
        let actual = tree.children("a").unwrap().collect::<HashSet<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn list_children_of_root() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("b", 2);
        tree.insert("b/c", 3);

        let expected = hashset![
            (RelativePathBuf::from("a"), &1),
            (RelativePathBuf::from("b"), &2),
        ];
        let actual = tree.children("").unwrap().collect::<HashSet<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn list_children_of_nonexistent_parent() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);

        assert!(matches!(tree.children("a/c"), None));
    }

    #[test]
    fn list_descendants() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.insert("a/b/c", 3);
        tree.insert("a/b/d", 4);

        let expected = hashset![
            (RelativePathBuf::from("a/b"), &2),
            (RelativePathBuf::from("a/b/c"), &3),
            (RelativePathBuf::from("a/b/d"), &4),
        ];
        let actual = tree.descendants("a").unwrap().collect::<HashSet<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn list_descendants_of_root() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("b", 2);
        tree.insert("b/c", 3);

        let expected = hashset![
            (RelativePathBuf::from("a"), &1),
            (RelativePathBuf::from("b"), &2),
            (RelativePathBuf::from("b/c"), &3),
        ];
        let actual = tree.descendants("").unwrap().collect::<HashSet<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn list_descendants_of_nonexistent_parent() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);

        assert!(matches!(tree.descendants("a/c"), None));
    }

    #[test]
    fn walk_tree() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.insert("a/b/c", 3);
        tree.insert("d", 4);

        let expected = hashset![
            (RelativePathBuf::from("a"), 1),
            (RelativePathBuf::from("a/b"), 2),
            (RelativePathBuf::from("a/b/c"), 3),
            (RelativePathBuf::from("d"), 4),
        ];

        let mut actual = HashSet::new();
        let result = tree.walk("", |entry| {
            actual.insert((entry.path, *entry.value));
            WalkPredicate::Continue
        });

        assert!(matches!(result, Some(false)));
        assert_eq!(actual, expected);
    }

    #[test]
    fn walk_tree_skip_descendants() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.insert("a/b/c", 3);
        tree.insert("a/b/c/d", 4);
        tree.insert("e", 5);

        let expected = hashset![
            (RelativePathBuf::from("a"), 1),
            (RelativePathBuf::from("a/b"), 2),
            (RelativePathBuf::from("e"), 5),
        ];

        let mut actual = HashSet::new();
        let result = tree.walk("", |entry| {
            actual.insert((entry.path.clone(), *entry.value));
            if &entry.path == "a/b" {
                WalkPredicate::SkipDescendants
            } else {
                WalkPredicate::Continue
            }
        });

        assert!(matches!(result, Some(false)));
        assert_eq!(actual, expected);
    }

    #[test]
    fn walk_tree_skip_siblings() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.insert("a/c", 3);
        tree.insert("a/d", 4);
        tree.insert("e", 5);

        let siblings = hashset![
            (RelativePathBuf::from("a/b"), 2),
            (RelativePathBuf::from("a/c"), 3),
            (RelativePathBuf::from("a/d"), 4),
        ];

        let mut actual = HashSet::new();
        let result = tree.walk("", |entry| {
            actual.insert((entry.path.clone(), *entry.value));
            if siblings.contains(&(entry.path, *entry.value)) {
                WalkPredicate::SkipSiblings
            } else {
                WalkPredicate::Continue
            }
        });

        assert!(matches!(result, Some(false)));
        assert_eq!(actual.len(), 3);
        assert!(actual.contains(&(RelativePathBuf::from("a"), 1)));
        assert!(actual.contains(&(RelativePathBuf::from("e"), 5)));
        assert_eq!(actual.intersection(&siblings).count(), 1)
    }

    #[test]
    fn walk_tree_stop() {
        let mut tree = PathTree::new();
        tree.insert("a", 1);
        tree.insert("a/b", 2);
        tree.insert("a/b/c", 3);
        tree.insert("a/b/c/d", 4);
        tree.insert("e", 5);

        let mut actual = HashSet::new();
        let result = tree.walk("", |entry| {
            if actual.len() == 2 {
                return WalkPredicate::Stop;
            }
            actual.insert((entry.path, *entry.value));
            WalkPredicate::Continue
        });

        assert!(matches!(result, Some(true)));
        assert_eq!(actual.len(), 2);
    }

    #[test]
    fn clear_tree() {
        let mut tree = PathTree::new();
        tree.insert("a", 1u32);
        tree.insert("a/b", 2u32);
        tree.clear();

        let actual = tree.descendants("").unwrap().collect::<Vec<_>>();
        let expected = Vec::<(RelativePathBuf, &u32)>::new();

        assert_eq!(expected, actual);
    }
}
