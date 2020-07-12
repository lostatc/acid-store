use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

use relative_path::{RelativePath, RelativePathBuf};
use walkdir::WalkDir;

/// A node in a `PathTree`.
#[derive(Debug, Default)]
struct PathNode<V> {
    /// The file name.
    name: String,

    /// The file's children.
    children: Vec<PathNode<V>>,

    /// The associated value.
    value: Option<V>,
}

/// A tree that associates file paths with values of type `V`.
#[derive(Debug, Default)]
pub struct PathTree<V> {
    nodes: Vec<PathNode<V>>,
}

impl<V> PathTree<V> {
    /// Return a new empty `PathTree`.
    pub fn new() -> Self {
        PathTree { nodes: Vec::new() }
    }

    /// Insert the given `path` and `value` into the tree.
    pub fn insert(&mut self, path: &RelativePath, value: V) -> bool {
        let mut current_nodes = &mut self.nodes;
        let mut current_value = &mut None;
        let mut exists = true;

        'segments: for segment in path.iter() {
            for (i, node) in current_nodes.iter().enumerate() {
                if segment == node.name {
                    let existing_node = current_nodes.get_mut(i).unwrap();
                    current_nodes = &mut existing_node.children;
                    current_value = &mut existing_node.value;
                    continue 'segments;
                }
            }

            exists = false;

            let new_node = PathNode {
                name: segment.to_string(),
                children: Vec::new(),
                value: None,
            };

            current_nodes.push(new_node);

            let new_node = current_nodes.last_mut().unwrap();
            current_value = &mut new_node.value;
            current_nodes = &mut new_node.children;
        }

        *current_value = Some(value);

        !exists
    }

    /// Return the value associated with `path` or `None` if there is none.
    pub fn get(&self, path: &RelativePath) -> Option<&V> {
        let mut nodes = &self.nodes;
        let mut value = None;

        'segments: for segment in path.iter() {
            for node in nodes {
                if segment == node.name {
                    value = node.value.as_ref();
                    nodes = &node.children;
                    continue 'segments;
                }
            }

            return None;
        }

        value
    }
}

fn tree_test() {
    let mut tree = PathTree::new();
    let paths = WalkDir::new("/home/wren").into_iter();

    for dir_entry in paths {
        let dir_entry = match dir_entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let relative_path =
            RelativePathBuf::from_path(dir_entry.path().strip_prefix("/home/wren").unwrap())
                .unwrap();
        tree.insert(&relative_path, 0);
    }

    println!("All done!");
    sleep(Duration::from_secs(300));
}

fn map_test() {
    let mut map = HashMap::new();
    let paths = WalkDir::new("/home/wren").into_iter();

    for dir_entry in paths {
        let dir_entry = match dir_entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let relative_path =
            RelativePathBuf::from_path(dir_entry.path().strip_prefix("/home/wren").unwrap())
                .unwrap();
        map.insert(relative_path, 0);
    }

    println!("All done!");
    sleep(Duration::from_secs(300));
}

fn main() {
    // 130 MiB
    // 140 s
    // tree_test()

    // 260 MiB
    // 61 s
    map_test()
}
