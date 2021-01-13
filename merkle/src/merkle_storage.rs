//! # MerkleStorage
//!
//! Storage for key/values with git-like semantics and history.
//!
//! # Data Structure
//! A storage with just one key `a/b/c` and its corresponding value `8` is represented like this:
//!
//! ``
//! [commit] ---> [tree1] --a--> [tree2] --b--> [tree3] --c--> [blob_8]
//! ``
//!
//! The db then contains the following:
//! ```no_compile
//! <hash_of_blob; blob8>
//! <hash_of_tree3, tree3>, where tree3 is a map {c: hash_blob8}
//! <hash_of_tree2, tree2>, where tree2 is a map {b: hash_of_tree3}
//! <hash_of_tree2, tree2>, where tree1 is a map {a: hash_of_tree2}
//! <hash_of_commit>; commit>, where commit points to the root tree (tree1)
//! ```
//!
//! Then, when looking for a path a/b/c in a spcific commit, we first get the hash of the root tree
//! from the commit, then get the tree from the database, get the hash of "a", look it up in the db,
//! get the hash of "b" from that tree, load from db, then get the hash of "c" and retrieve the
//! final value.
//!
//!
//! Now, let's assume we want to add a path `X` also referencing the value `8`. That creates a new
//! tree that reuses the previous subtree for `a/b/c` and branches away from root for `X`:
//!
//! ```no_compile
//! [tree1] --a--> [tree2] --b--> [tree3] --c--> [blob_8]
//!                   ^                             ^
//!                   |                             |
//! [tree_X]----a-----                              |
//!     |                                           |
//!      ----------------------X--------------------
//! ```
//!
//! The following is added to the database:
//! ``
//! <hash_of_tree_X; tree_X>, where tree_X is a map {a: hash_of_tree2, X: hash_of_blob8}
//! ``
//!
//! Reference: https://git-scm.com/book/en/v2/Git-Internals-Git-Objects
use std::hash::Hash;
use serde::Deserialize;
use serde::Serialize;
use std::collections::{HashMap, BTreeMap};
use im::{OrdMap, HashSet};
use failure::Fail;
use std::sync::{Arc, RwLock, RwLockWriteGuard, Mutex};
use std::time::Instant;
use crate::hash::HashType;
use std::convert::TryInto;
use sodiumoxide::crypto::generichash::State;
use crate::codec::BincodeEncoded;
use crate::schema::KeyValueSchema;
use crate::database::{KeyValueStoreWithSchema, Batch, DB, DBStats, IteratorMode};
use crate::database::DBError;
use linked_hash_set::LinkedHashSet;
use crate::ivec::IVec;
use std::array::TryFromSliceError;
use blake2::VarBlake2b;
use blake2::digest::{Update, VariableOutput};
use crate::database;
use pprof::protos::*;
use std::fs::File;
use std::io::Write;

const HASH_LEN: usize = 32;

pub type ContextKey = Vec<String>;
pub type ContextValue = Vec<u8>;
pub type EntryHash = [u8; HASH_LEN];

#[derive(Clone, Debug, Serialize, Deserialize)]
enum NodeKind {
    NonLeaf,
    Leaf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Node {
    node_kind: NodeKind,
    entry_hash: EntryHash,
}

// Tree must be an ordered structure for consistent hash in hash_tree
// Currently immutable OrdMap is used to allow cloning trees without too much overhead
type Tree = OrdMap<String, Node>;

#[derive(Debug, Hash, Clone, Serialize, Deserialize)]
struct Commit {
    parent_commit_hash: Option<EntryHash>,
    root_hash: EntryHash,
    time: u64,
    author: String,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Entry {
    Tree(Tree),
    Blob(ContextValue),
    Commit(Commit),
}

pub type MerkleStorageKV = dyn KeyValueStoreWithSchema<MerkleStorage> + Sync + Send;

pub struct MerkleStorage {
    /// tree with current staging area (currently checked out context)
    current_stage_tree: Option<Tree>,
    db: Arc<RwLock<MerkleStorageKV>>,
    /// all entries in current staging area
    staged: HashMap<EntryHash, Entry>,
    last_commit_hash: Option<EntryHash>,
    /// storage latency statistics
    perf_stats: MerklePerfStats,
}

#[derive(Debug, Fail)]
pub enum MerkleError {
    /// External libs errors
    #[fail(display = "RocksDB error: {:?}", error)]
    DBError { error: database::DBError },
    #[fail(display = "Serialization error: {:?}", error)]
    SerializationError { error: bincode::Error },

    /// Internal unrecoverable bugs that should never occur
    #[fail(display = "No root retrieved for this commit!")]
    CommitRootNotFound,
    #[fail(display = "Cannot commit without a predecessor!")]
    MissingAncestorCommit,
    #[fail(display = "There is a commit or three under key {:?}, but not a value!", key)]
    ValueIsNotABlob { key: String },
    #[fail(display = "Found wrong structure. Was looking for {}, but found {}", sought, found)]
    FoundUnexpectedStructure { sought: String, found: String },
    #[fail(display = "Entry not found! Hash={}", hash)]
    EntryNotFound { hash: String },

    /// Wrong user input errors
    #[fail(display = "No value under key {:?}.", key)]
    ValueNotFound { key: String },
    #[fail(display = "Cannot search for an empty key.")]
    KeyEmpty,
    #[fail(display = "Failed to convert hash to array: {}", error)]
    HashConversionError { error: TryFromSliceError },
}

impl From<database::DBError> for MerkleError {
    fn from(error: database::DBError) -> Self { MerkleError::DBError { error } }
}

impl From<bincode::Error> for MerkleError {
    fn from(error: bincode::Error) -> Self { MerkleError::SerializationError { error } }
}

impl From<TryFromSliceError> for MerkleError {
    fn from(error: TryFromSliceError) -> Self { MerkleError::HashConversionError { error } }
}

/// Latency statistics for each action (in nanoseconds)
#[derive(Serialize, Debug, Clone, Copy)]
pub struct OperationLatencies {
    /// divide this by the next field to get avg (mean) time spent in operation
    cumul_op_exec_time: f64,
    pub op_exec_times: u64,
    pub avg_exec_time: f64,
    /// lowest time spent in operation
    pub op_exec_time_min: f64,
    /// highest time spent in operation
    pub op_exec_time_max: f64,
}

impl OperationLatencies {
    pub fn new() -> Self {
        OperationLatencies {
            cumul_op_exec_time: 0.0,
            op_exec_times: 0,
            avg_exec_time: 0.0,
            op_exec_time_min: f64::MAX,
            op_exec_time_max: f64::MIN,
        }
    }
}

// Latency statistics indexed by operation name (e.g. "Set")
pub type OperationLatencyStats = HashMap<String, OperationLatencies>;

// Latency statistics per path indexed by first chunk of path (under /data/)
pub type PerPathOperationStats = HashMap<String, OperationLatencyStats>;

#[derive(Serialize, Debug, Clone)]
pub struct MerklePerfStats {
    pub global: OperationLatencyStats,
    pub perpath: PerPathOperationStats,
}

#[derive(Serialize, Debug, Clone)]
pub struct MerkleStorageStats {
    pub db_stats: DBStats,
    pub perf_stats: MerklePerfStats,
}

impl BincodeEncoded for EntryHash {}

impl KeyValueSchema for MerkleStorage {
    // keys is hash of Entry
    type Key = EntryHash;
    // Entry (serialized)
    type Value = Vec<u8>;

    #[inline]
    fn name() -> &'static str {
        "merkle_storage"
    }
}

// Tree in String form needed for JSON RPCs
pub type StringTreeMap = BTreeMap<String, StringTreeEntry>;

/// Tree in String form needed for JSON RPCs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StringTreeEntry {
    Tree(StringTreeMap),
    Blob(String),
    Null,
}

impl MerkleStorage {
    pub fn new(db: Arc<RwLock<MerkleStorageKV>>) -> Self {
        MerkleStorage {
            db,
            staged: HashMap::new(),
            current_stage_tree: None,
            last_commit_hash: None,
            perf_stats: MerklePerfStats { global: HashMap::new(), perpath: HashMap::new() },
        }
    }

    /// Get value from current staged root
    pub fn get(&mut self, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let instant = Instant::now();
        let root = &self.get_staged_root()?;
        let root_hash = self.hash_tree(&root)?;

        let rv = self.get_from_tree(&root_hash, key);
        self.update_execution_stats("Get".to_string(), Some(&key), &instant);
        rv
    }

    /// Get value. Staging area is checked first, then last (checked out) commit.
    pub fn get_by_prefix(&mut self, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let root = self.get_staged_root()?;
        self._get_key_values_by_prefix(root, prefix)
    }

    /// Get value from historical context identified by commit hash.
    pub fn get_history(&mut self, commit_hash: &EntryHash, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let instant = Instant::now();
        let commit = self.get_commit(commit_hash)?;

        let rv = self.get_from_tree(&commit.root_hash, key);
        self.update_execution_stats("GetKeyFromHistory".to_string(), Some(&key), &instant);
        rv
    }

    fn get_from_tree(&self, root_hash: &EntryHash, key: &ContextKey) -> Result<ContextValue, MerkleError> {
        let mut full_path = key.clone();
        let file = full_path.pop().ok_or(MerkleError::KeyEmpty)?;
        let path = full_path;
        // find tree by path
        let root = self.get_tree(root_hash)?;
        let node = self.find_tree(&root, &path)?;

        // get file node from tree
        let node = match node.get(&file) {
            None => return Err(MerkleError::ValueNotFound { key: self.key_to_string(key) }),
            Some(entry) => entry,
        };
        // get blob by hash
        match self.get_entry(&node.entry_hash)? {
            Entry::Blob(blob) => Ok(blob),
            _ => Err(MerkleError::ValueIsNotABlob { key: self.key_to_string(key) })
        }
    }

    // TODO: recursion is risky (stack overflow) and inefficient, try to do it iteratively..
    fn get_key_values_from_tree_recursively(&self, path: &str, entry: &Entry, entries: &mut Vec<(ContextKey, ContextValue)>) -> Result<(), MerkleError> {
        match entry {
            Entry::Blob(blob) => {
                // push key-value pair
                entries.push((self.string_to_key(path), blob.to_vec()));
                Ok(())
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                tree.iter().map(|(key, child_node)| {
                    let fullpath = path.to_owned() + "/" + key;
                    match self.get_entry(&child_node.entry_hash) {
                        Err(_) => Ok(()),
                        Ok(entry) => self.get_key_values_from_tree_recursively(&fullpath, &entry, entries),
                    }
                }).find_map(|res| {
                    match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    }
                }).unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => {
                match self.get_entry(&commit.root_hash) {
                    Err(err) => Err(err),
                    Ok(entry) => self.get_key_values_from_tree_recursively(path, &entry, entries),
                }
            }
        }
    }

    pub fn gc(&mut self) -> Result<(),MerkleError>{
        let guard = pprof::ProfilerGuard::new(100).unwrap();
        let instant = Instant::now();
        let db = self.db.clone();
        // Lock write to database
        let mut db_writer  = db.write().unwrap();
        let mut todo = LinkedHashSet::new();
        self.mark_entries(&mut todo, &mut db_writer);
        self.sweep_entries(&mut db_writer,todo);
        self.update_execution_stats("GC".to_string(), None, &instant);
        match guard.report().build() {
            Ok(report) => {
                let mut file = File::create("./merkle-gc-profile.pb").unwrap();
                let profile = report.pprof().unwrap();
                let mut content = Vec::new();
                profile.encode(&mut content).unwrap();
                file.write_all(&content).unwrap();

                println!("report: {:#?}", &report);
            }
            Err(_) => {}
        };
        Ok(())
    }

    fn mark_entries(&self, todo : &mut LinkedHashSet<Vec<u8>>, db: &mut RwLockWriteGuard<MerkleStorageKV>) {
        if let Some(entry_hash) = &self.last_commit_hash {
            if let Ok(entry) = self.get_entry_from_db_with_write_lock(entry_hash, db) {
                self.mark_entries_recursively(&entry,todo, db);
            }
        }
    }

    fn sweep_entries(&self, db: &mut RwLockWriteGuard<MerkleStorageKV>, todo : LinkedHashSet<Vec<u8>>)  -> Result<(),MerkleError> {
        let p = todo.into_iter().collect::<Vec<_>>();
        db.retain(p);
        Ok(())
    }

    #[inline]
    fn mark_entries_recursively(&self,  entry: &Entry, todo : &mut LinkedHashSet<Vec<u8>>,db: &mut RwLockWriteGuard<MerkleStorageKV>)  {
        if let Ok(k) = &self.hash_entry(entry) {
            match entry {
                Entry::Blob(_) => {
                    todo.insert_if_absent(k.to_vec());
                }
                Entry::Tree(tree) => {
                    todo.insert_if_absent(k.to_vec());
                    tree.iter().for_each(|(key, child_node)| {
                        match self.get_entry_from_db_with_write_lock(&child_node.entry_hash, db) {
                            Err(_) => {}
                            Ok(entry) => self.mark_entries_recursively(&entry, todo, db),
                        };
                    });
                }
                Entry::Commit(commit) => {
                    todo.insert_if_absent(k.to_vec());
                    match self.get_entry_from_db_with_write_lock(&commit.root_hash, db) {
                        Err(_) => {}
                        Ok(entry) => {
                            self.mark_entries_recursively(&entry, todo,db)
                        }
                    }
                }
            }
        }

    }

    fn get_entry_from_db_with_write_lock(&self, hash: &EntryHash, db : &mut RwLockWriteGuard<MerkleStorageKV>) -> Result<Entry, MerkleError> {
        let entry_bytes = db.get(hash)?;
        match entry_bytes {
            None => Err(MerkleError::EntryNotFound { hash: HashType::ContextHash.hash_to_b58check(hash) }),
            Some(entry_bytes) => {
                Ok(bincode::deserialize(entry_bytes.as_ref())?)
            }
        }
    }

    /// Go recursively down the tree from Entry, build string tree and return it
    /// (or return hex value if Blob)
    fn get_context_recursive(&self, path: &str, entry: &Entry, depth: Option<usize>) -> Result<StringTreeEntry, MerkleError> {
        if let Some(0) = depth {
            return Ok(StringTreeEntry::Null)
        }

        match entry {
            Entry::Blob(blob) => {
                Ok(StringTreeEntry::Blob(hex::encode(blob)))
            }
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                let mut new_tree = StringTreeMap::new();
                for (key, child_node) in tree.iter() {
                    let fullpath = path.to_owned() + "/" + key;
                    let e = self.get_entry(&child_node.entry_hash)?;
                    let rdepth = depth.map(|d| d-1);
                    new_tree.insert(key.to_owned(), self.get_context_recursive(&fullpath, &e, rdepth)?);
                }
                Ok(StringTreeEntry::Tree(new_tree))
            }
            Entry::Commit(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "Tree/Blob".to_string(),
                found: "Commit".to_string(),
            })
        }
    }

    /// Get context tree under given prefix in string form (for JSON)
    /// depth - None returns full tree
    pub fn get_context_tree_by_prefix(&mut self, context_hash: &EntryHash, prefix: &ContextKey, depth: Option<usize>) -> Result<StringTreeEntry, MerkleError> {
        if let Some(0) = depth {
            return Ok(StringTreeEntry::Null)
        }

        let instant = Instant::now();
        let mut out = StringTreeMap::new();
        let commit = self.get_commit(context_hash)?;
        let root_tree = self.get_tree(&commit.root_hash)?;
        let prefixed_tree = self.find_tree(&root_tree, prefix)?;

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.get_entry(&child_node.entry_hash)?;
            let delimiter: &str;
            if prefix.is_empty() {
                delimiter = "";
            } else {
                delimiter = "/";
            }

            // construct full path as Tree key is only one chunk of it
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            let rdepth = depth.map(|d| d-1);
            out.insert(key.to_owned(), self.get_context_recursive(&fullpath, &entry, rdepth)?);
        }

        self.update_execution_stats("GetContextTreeByPrefix".to_string(), Some(&prefix), &instant);
        Ok(StringTreeEntry::Tree(out))
    }

    /// Construct Vec of all context key-values under given prefix
    pub fn get_key_values_by_prefix(&mut self, context_hash: &EntryHash, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let instant = Instant::now();
        let commit = self.get_commit(context_hash)?;
        let root_tree = self.get_tree(&commit.root_hash)?;
        let rv = self._get_key_values_by_prefix(root_tree, prefix);
        self.update_execution_stats("GetKeyValuesByPrefix".to_string(), Some(&prefix), &instant);
        rv
    }

    fn _get_key_values_by_prefix(&self, root_tree: Tree, prefix: &ContextKey) -> Result<Option<Vec<(ContextKey, ContextValue)>>, MerkleError> {
        let prefixed_tree = self.find_tree(&root_tree, prefix)?;
        let mut keyvalues: Vec<(ContextKey, ContextValue)> = Vec::new();

        for (key, child_node) in prefixed_tree.iter() {
            let entry = self.get_entry(&child_node.entry_hash)?;
            let delimiter: &str;
            if prefix.is_empty() {
                delimiter = "";
            } else {
                delimiter = "/";
            }
            // construct full path as Tree key is only one chunk of it
            let fullpath = self.key_to_string(prefix) + delimiter + key;
            self.get_key_values_from_tree_recursively(&fullpath, &entry, &mut keyvalues)?;
        }

        if keyvalues.is_empty() {
            Ok(None)
        } else {
            Ok(Some(keyvalues))
        }
    }

    /// Flush the staging area and and move to work on a certain commit from history.
    pub fn checkout(&mut self, context_hash: &EntryHash) -> Result<(), MerkleError> {
        let instant = Instant::now();
        let commit = self.get_commit(&context_hash)?;
        self.current_stage_tree = Some(self.get_tree(&commit.root_hash)?);
        self.last_commit_hash = Some(*context_hash);
        self.staged = HashMap::new();
        self.update_execution_stats("Checkout".to_string(), None, &instant);
        Ok(())
    }

    /// Take the current changes in the staging area, create a commit and persist all changes
    /// to database under the new commit. Return last commit if there are no changes, that is
    /// empty commits are not allowed.
    pub fn commit(&mut self,
                  time: u64,
                  author: String,
                  message: String,
    ) -> Result<EntryHash, MerkleError> {
        let instant = Instant::now();
        let staged_root = self.get_staged_root()?;
        let staged_root_hash = self.hash_tree(&staged_root)?;
        let parent_commit_hash = self.last_commit_hash;

        let new_commit = Commit {
            root_hash: staged_root_hash,
            parent_commit_hash,
            time,
            author,
            message,
        };
        let entry = Entry::Commit(new_commit.clone());

        let new_commit_hash = self.hash_commit(&new_commit)?;
        self.put_to_staging_area(&new_commit_hash, entry.clone());
        self.persist_staged_entry_to_db(&entry)?;
        self.staged = HashMap::new();
        self.last_commit_hash = Some(new_commit_hash);
        self.update_execution_stats("Commit".to_string(), None, &instant);
        Ok(new_commit_hash)
    }

    /// Set key/val to the staging area.
    pub fn set(&mut self, key: &ContextKey, value: &ContextValue) -> Result<(), MerkleError> {
        let instant = Instant::now();
        let root = self.get_staged_root()?;
        let new_root_hash = &self._set(&root, key, value)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        self.update_execution_stats("Set".to_string(), Some(&key), &instant);
        Ok(())
    }

    /// Walk down the tree to find key, set new value and walk back up recalculating hashes -
    /// return new top hash of tree. Note: no writes to DB yet
    fn _set(&mut self, root: &Tree, key: &ContextKey, value: &ContextValue) -> Result<EntryHash, MerkleError> {
        let blob_hash = self.hash_blob(&value)?;
        self.put_to_staging_area(&blob_hash, Entry::Blob(value.clone()));
        let new_node = Node { entry_hash: blob_hash, node_kind: NodeKind::Leaf };
        self.compute_new_root_with_change(root, &key, Some(new_node))
    }

    /// Delete an item from the staging area.
    pub fn delete(&mut self, key: &ContextKey) -> Result<(), MerkleError> {
        let instant = Instant::now();
        let root = self.get_staged_root()?;
        let new_root_hash = &self._delete(&root, key)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        self.update_execution_stats("Delete".to_string(), Some(&key), &instant);
        Ok(())
    }

    fn _delete(&mut self, root: &Tree, key: &ContextKey) -> Result<EntryHash, MerkleError> {
        if key.is_empty() { return self.hash_tree(root); }

        self.compute_new_root_with_change(root, &key, None)
    }

    /// Copy subtree under a new path.
    /// TODO Consider copying values!
    pub fn copy(&mut self, from_key: &ContextKey, to_key: &ContextKey) -> Result<(), MerkleError> {
        let instant = Instant::now();
        let root = self.get_staged_root()?;
        let new_root_hash = &self._copy(&root, from_key, to_key)?;
        self.current_stage_tree = Some(self.get_tree(new_root_hash)?);
        // TODO: do we need to include from_key in stats?
        self.update_execution_stats("CopyToDiff".to_string(), Some(&to_key), &instant);
        Ok(())
    }

    fn _copy(&mut self, root: &Tree, from_key: &ContextKey, to_key: &ContextKey) -> Result<EntryHash, MerkleError> {
        let source_tree = self.find_tree(root, &from_key)?;
        let source_tree_hash = self.hash_tree(&source_tree)?;
        Ok(self.compute_new_root_with_change(
            &root, &to_key, Some(self.get_non_leaf(source_tree_hash)))?)
    }

    /// Get a new tree with `new_entry_hash` put under given `key`.
    ///
    /// # Arguments
    ///
    /// * `root` - Tree to modify
    /// * `key` - path under which the changes takes place
    /// * `new_entry_hash` - None for deletion, Some for inserting a hash under the key.
    fn compute_new_root_with_change(&mut self,
                                    root: &Tree,
                                    key: &[String],
                                    new_node: Option<Node>,
    ) -> Result<EntryHash, MerkleError> {
        if key.is_empty() {
            match new_node {
                Some(n) => return Ok(n.entry_hash),
                None => {
                    let tree_hash = self.hash_tree(root)?;
                    return Ok(self.get_non_leaf(tree_hash).entry_hash);
                }
            }
        }

        let last = key.last().unwrap();
        let path = &key[..key.len() - 1];
        // find tree by path and get new copy of it
        let mut tree = self.find_tree(root, path)?;

        // make the modification at key
        match new_node {
            None => tree.remove(last),
            Some(new_node) => {
                tree.insert(last.clone(), new_node)
            }
        };

        if tree.is_empty() {
            // last element was removed, delete this node
            self.compute_new_root_with_change(root, path, None)
        } else {
            let new_tree_hash = self.hash_tree(&tree)?;
            // put new version of the tree to staging area
            // note: the old version is kept in staging area
            self.put_to_staging_area(&new_tree_hash, Entry::Tree(tree));
            self.compute_new_root_with_change(
                root, path, Some(self.get_non_leaf(new_tree_hash)))
        }
    }

    /// Find tree by path and return a copy. Return an empty tree if no tree under this path exists or if a blob
    /// (= value) is encountered along the way.
    ///
    /// # Arguments
    ///
    /// * `root` - reference to a tree in which we search
    /// * `key` - sought path
    fn find_tree(&self, root: &Tree, key: &[String]) -> Result<Tree, MerkleError> {
        // terminate recursion if end of path was reached
        if key.is_empty() { return Ok(root.clone()); }

        // first get node at key
        let child_node = match root.get(key.first().unwrap()) {
            Some(hash) => hash,
            None => return Ok(Tree::new()),
        };

        // get entry by hash (from staged area or DB)
        match self.get_entry(&child_node.entry_hash)? {
            Entry::Tree(tree) => {
                self.find_tree(&tree, &key[1..])
            }
            Entry::Blob(_) => Ok(Tree::new()),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            })
        }
    }

    /// Get latest staged tree. If it's empty, init genesis  and return genesis root.
    fn get_staged_root(&mut self) -> Result<Tree, MerkleError> {
        match &self.current_stage_tree {
            None => {
                let tree = Tree::new();
                self.put_to_staging_area(&self.hash_tree(&tree)?, Entry::Tree(tree.clone()));
                Ok(tree)
            }
            Some(tree) => {
                Ok(tree.clone())
            }
        }
    }

    fn put_to_staging_area(&mut self, key: &EntryHash, value: Entry) {
        self.staged.insert(*key, value);
    }

    /// Persists an entry and its descendants from staged area to database on disk.
    fn persist_staged_entry_to_db(&self, entry: &Entry) -> Result<(), MerkleError> {
        let mut batch = Batch::default(); // batch containing DB key values to persist

        // build list of entries to be persisted
        self.get_entries_recursively(entry, &mut batch)?;

        // atomically write all entries in one batch to DB
        self.db.write().unwrap().write_batch(batch)?;

        Ok(())
    }

    /// Builds vector of entries to be persisted to DB, recursively
    fn get_entries_recursively(&self, entry: &Entry, batch: &mut Batch) -> Result<(), MerkleError> {
        // add entry to batch
        let k = &self.hash_entry(entry)?;
        let v = bincode::serialize(entry)?;
        self.db.write().unwrap().put_batch(batch, k, &v);

        match entry {
            Entry::Blob(_) => Ok(()),
            Entry::Tree(tree) => {
                // Go through all descendants and gather errors. Remap error if there is a failure
                // anywhere in the recursion paths. TODO: is revert possible?
                tree.iter().map(|(_, child_node)| {
                    match self.staged.get(&child_node.entry_hash) {
                        None => Ok(()),
                        Some(entry) => self.get_entries_recursively(entry, batch),
                    }
                }).find_map(|res| {
                    match res {
                        Ok(_) => None,
                        Err(err) => Some(Err(err)),
                    }
                }).unwrap_or(Ok(()))
            }
            Entry::Commit(commit) => {
                match self.get_entry(&commit.root_hash) {
                    Err(err) => Err(err),
                    Ok(entry) => self.get_entries_recursively(&entry, batch),
                }
            }
        }
    }

    fn hash_entry(&self, entry: &Entry) -> Result<EntryHash, MerkleError> {
        match entry {
            Entry::Commit(commit) => self.hash_commit(&commit),
            Entry::Tree(tree) => self.hash_tree(&tree),
            Entry::Blob(blob) => self.hash_blob(blob),
        }
    }

    fn hash_commit(&self, commit: &Commit) -> Result<EntryHash, MerkleError> {
        let mut hasher = VarBlake2b::new(HASH_LEN).unwrap();
        hasher.update(&(HASH_LEN as u64).to_be_bytes());
        hasher.update(&commit.root_hash);

        if commit.parent_commit_hash.is_none() {
            hasher.update(&(0_u64).to_be_bytes());
        } else {
            hasher.update(&(1_u64).to_be_bytes()); // # of parents; we support only 1
            hasher.update(&(commit.parent_commit_hash.unwrap().len() as u64).to_be_bytes());
            hasher.update(&commit.parent_commit_hash.unwrap());
        }
        hasher.update(&(commit.time as u64).to_be_bytes());
        hasher.update(&(commit.author.len() as u64).to_be_bytes());
        hasher.update(&commit.author.clone().into_bytes());
        hasher.update(&(commit.message.len() as u64).to_be_bytes());
        hasher.update(&commit.message.clone().into_bytes());

        Ok(hasher.finalize_boxed().as_ref().try_into()?)
    }

    fn hash_tree(&self, tree: &Tree) -> Result<EntryHash, MerkleError> {
        let mut hasher = VarBlake2b::new(HASH_LEN).unwrap();

        hasher.update(&(tree.len() as u64).to_be_bytes());
        tree.iter().for_each(|(k, v)| {
            hasher.update(&self.encode_irmin_node_kind(&v.node_kind));
            hasher.update(&[k.len() as u8]);
            hasher.update(&k.clone().into_bytes());
            hasher.update(&(HASH_LEN as u64).to_be_bytes());
            hasher.update(&v.entry_hash);
        });

        Ok(hasher.finalize_boxed().as_ref().try_into()?)
    }

    fn hash_blob(&self, blob: &ContextValue) -> Result<EntryHash, MerkleError> {
        let mut hasher = VarBlake2b::new(HASH_LEN).unwrap();
        hasher.update(&(blob.len() as u64).to_be_bytes());
        hasher.update(blob);

        Ok(hasher.finalize_boxed().as_ref().try_into()?)
    }

    fn encode_irmin_node_kind(&self, kind: &NodeKind) -> [u8; 8] {
        match kind {
            NodeKind::NonLeaf => [0, 0, 0, 0, 0, 0, 0, 0],
            NodeKind::Leaf => [255, 0, 0, 0, 0, 0, 0, 0],
        }
    }


    fn get_tree(&self, hash: &EntryHash) -> Result<Tree, MerkleError> {
        match self.get_entry(hash)? {
            Entry::Tree(tree) => Ok(tree),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "blob".to_string(),
            }),
            Entry::Commit { .. } => Err(MerkleError::FoundUnexpectedStructure {
                sought: "tree".to_string(),
                found: "commit".to_string(),
            }),
        }
    }

    fn get_commit(&self, hash: &EntryHash) -> Result<Commit, MerkleError> {
        match self.get_entry(hash)? {
            Entry::Commit(commit) => Ok(commit),
            Entry::Tree(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "tree".to_string(),
            }),
            Entry::Blob(_) => Err(MerkleError::FoundUnexpectedStructure {
                sought: "commit".to_string(),
                found: "blob".to_string(),
            }),
        }
    }

    /// Get entry from staging area or look up in DB if not found
    fn get_entry(&self, hash: &EntryHash) -> Result<Entry, MerkleError> {
        match self.staged.get(hash) {
            None => {
                let entry_bytes = self.db.read().unwrap().get(hash)?;
                match entry_bytes {
                    None => Err(MerkleError::EntryNotFound { hash: HashType::ContextHash.hash_to_b58check(hash) }),
                    Some(entry_bytes) => Ok(bincode::deserialize(&entry_bytes)?),
                }
            }
            Some(entry) => Ok(entry.clone()),
        }
    }

    fn get_non_leaf(&self, hash: EntryHash) -> Node {
        Node { node_kind: NodeKind::NonLeaf, entry_hash: hash }
    }

    /// Convert key in array form to string form
    fn key_to_string(&self, key: &ContextKey) -> String {
        key.join("/")
    }

    /// Convert key in string form to array form
    fn string_to_key(&self, string: &str) -> ContextKey {
        string.split('/').map(str::to_string).collect()
    }

    /// Get last committed hash
    pub fn get_last_commit_hash(&self) -> Option<EntryHash> {
        self.last_commit_hash
    }

    /// Get various merkle storage statistics
    pub fn get_merkle_stats(&self) -> Result<MerkleStorageStats, MerkleError> {
        let db_stats = self.db.read().unwrap().get_mem_use_stats()?;

        // calculate average values for global stats
        let mut perf = self.perf_stats.clone();
        for (_, stat) in perf.global.iter_mut() {
            if stat.op_exec_times > 0 {
                stat.avg_exec_time = stat.cumul_op_exec_time / (stat.op_exec_times as f64);
            } else {
                stat.avg_exec_time = 0.0;
            }
        }
        // calculate average values for per-path stats
        for (_node, stat) in perf.perpath.iter_mut() {
            for (_op, stat) in stat.iter_mut() {
                if stat.op_exec_times > 0 {
                    stat.avg_exec_time = stat.cumul_op_exec_time / (stat.op_exec_times as f64);
                } else {
                    stat.avg_exec_time = 0.0;
                }
            }
        }
        Ok(MerkleStorageStats { db_stats, perf_stats: perf })
    }

    /// Update global and per-path execution stats. Pass Instant with operation execution time
    pub fn update_execution_stats(&mut self, op: String, path: Option<&ContextKey>, instant: &Instant) {
        // stop timer and get duration
        let exec_time: f64 = instant.elapsed().as_nanos() as f64;

        // collect global stats
        let entry = self.perf_stats.global.entry(op.to_owned()).or_insert( OperationLatencies::new() );
        // add to cumulative execution time
        entry.cumul_op_exec_time += exec_time;
        entry.op_exec_times += 1;

        // update min/max times for op
        if exec_time < entry.op_exec_time_min {
            entry.op_exec_time_min = exec_time;
        }
        if exec_time > entry.op_exec_time_max {
            entry.op_exec_time_max = exec_time;
        }

        // collect per-path stats
        if let Some(path) = path {
            // we are only interested in nodes under /data
            if path.len() > 1 && path[0] == "data".to_string() {
                let node = path[1].to_string();
                let perpath = self.perf_stats.perpath.entry(node).or_insert( HashMap::new() );
                let entry = perpath.entry(op).or_insert( OperationLatencies::new() );

                // add to cumulative execution time
                entry.cumul_op_exec_time += exec_time;
                entry.op_exec_times += 1;

                // update min/max times for op
                if exec_time < entry.op_exec_time_min {
                    entry.op_exec_time_min = exec_time;
                }
                if exec_time > entry.op_exec_time_max {
                    entry.op_exec_time_max = exec_time;
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(unused_must_use)]
mod tests {
    use super::*;
    use serial_test::serial;
    use crate::database::{DB};
    use assert_json_diff::assert_json_eq;
    /*
    * Tests need to run sequentially, otherwise they will try to open RocksDB at the same time.
    */
    fn get_storage() -> MerkleStorage { MerkleStorage::new(Arc::new(RwLock::new(DB::new()))) }

    fn clean_db() {
    }

    #[test]
    fn test_tree_hash() {
        let mut storage = get_storage();
        storage.set(&vec!["a".to_string(), "foo".to_string()], &vec![97, 98, 99]); // abc
        storage.set(&vec!["b".to_string(), "boo".to_string()], &vec![97, 98]);
        storage.set(&vec!["a".to_string(), "aaa".to_string()], &vec![97, 98, 99, 100]);
        storage.set(&vec!["x".to_string()], &vec![97]);
        storage.set(&vec!["one".to_string(), "two".to_string(), "three".to_string()], &vec![97]);
        let tree = storage.current_stage_tree.clone().unwrap();

        let hash = storage.hash_tree(&tree).unwrap();

        assert_eq!([0xDB, 0xAE, 0xD7, 0xB6], hash[0..4]);
    }

    #[test]
    fn test_commit_hash() {
        let mut storage = get_storage();
        storage.set(&vec!["a".to_string()], &vec![97, 98, 99]);

        let commit = storage.commit(
            0, "Tezos".to_string(), "Genesis".to_string());

        assert_eq!([0xCF, 0x95, 0x18, 0x33], commit.unwrap()[0..4]);

        storage.set(&vec!["data".to_string(), "x".to_string()], &vec![97]);
        let commit = storage.commit(
            0, "Tezos".to_string(), "".to_string());

        assert_eq!([0xCA, 0x7B, 0xC7, 0x02], commit.unwrap()[0..4]);
        // full irmin hash: ca7bc7022ffbd35acc97f7defb00c486bb7f4d19a2d62790d5949775eb74f3c8
    }

    #[test]
    fn test_multiple_commit_hash() {
        let mut storage = get_storage();
        let _commit = storage.commit(
            0, "Tezos".to_string(), "Genesis".to_string());

        storage.set(&vec!["data".to_string(), "a".to_string(), "x".to_string()], &vec![97]);
        storage.copy(&vec!["data".to_string(), "a".to_string()], &vec!["data".to_string(), "b".to_string()]);
        storage.delete(&vec!["data".to_string(), "b".to_string(), "x".to_string()]);
        let commit = storage.commit(
            0, "Tezos".to_string(), "".to_string());

        assert_eq!([0x9B, 0xB0, 0x0D, 0x6E], commit.unwrap()[0..4]);
    }

    #[test]
    fn get_test() {

        let mut storage = get_storage();

        let commit1;
        let commit2;
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        let key_eab: &ContextKey = &vec!["e".to_string(), "a".to_string(), "b".to_string()];
        let key_az: &ContextKey = &vec!["a".to_string(), "z".to_string()];
        let key_d: &ContextKey = &vec!["d".to_string()];

        {
            storage.set(key_abc, &vec![1u8, 2u8]);
            storage.set(key_abx, &vec![3u8]);
            assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8, 2u8]);
            assert_eq!(storage.get(&key_abx).unwrap(), vec![3u8]);
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

            storage.set(key_az, &vec![4u8]);
            storage.set(key_abx, &vec![5u8]);
            storage.set(key_d, &vec![6u8]);
            storage.set(key_eab, &vec![7u8]);
            assert_eq!(storage.get(key_abx).unwrap(), vec![5u8]);
            commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        assert_eq!(storage.get_history(&commit1, key_abc).unwrap(), vec![1u8, 2u8]);
        assert_eq!(storage.get_history(&commit1, key_abx).unwrap(), vec![3u8]);
        assert_eq!(storage.get_history(&commit2, key_abx).unwrap(), vec![5u8]);
        assert_eq!(storage.get_history(&commit2, key_az).unwrap(), vec![4u8]);
        assert_eq!(storage.get_history(&commit2, key_d).unwrap(), vec![6u8]);
        assert_eq!(storage.get_history(&commit2, key_eab).unwrap(), vec![7u8]);
    }

    #[test]
    fn test_copy() {
        clean_db();

        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        storage.set(key_abc, &vec![1_u8]);
        storage.copy(&vec!["a".to_string()], &vec!["z".to_string()]);

        assert_eq!(
            vec![1_u8],
            storage.get(&vec!["z".to_string(), "b".to_string(), "c".to_string()]).unwrap());
        // TODO test copy over commits
    }

    #[test]
    fn test_delete() {
        clean_db();
        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        storage.set(key_abc, &vec![2_u8]);
        storage.set(key_abx, &vec![3_u8]);
        storage.delete(key_abx);
        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

        assert!(storage.get_history(&commit1, &key_abx).is_err());
    }

    #[test]
    fn test_deleted_entry_available() {
        let db_name = "ms_test_deleted_entry_available";
        clean_db();

        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        storage.set(key_abc, &vec![2_u8]);
        let commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        storage.delete(key_abc);
        let _commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

        assert_eq!(vec![2_u8], storage.get_history(&commit1, &key_abc).unwrap());
    }

    #[test]
    fn test_delete_in_separate_commit() {
        let mut storage = get_storage();
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        storage.set(key_abc, &vec![2_u8]).unwrap();
        storage.set(key_abx, &vec![3_u8]).unwrap();
        storage.commit(0, "".to_string(), "".to_string()).unwrap();

        storage.delete(key_abx);
        let commit2 = storage.commit(
            0, "".to_string(), "".to_string()).unwrap();

        assert!(storage.get_history(&commit2, &key_abx).is_err());
    }

    #[test]
    fn test_checkout() {
        let mut storage = get_storage();

        let commit1;
        let commit2;
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];

        {
            storage.set(key_abc, &vec![1u8]).unwrap();
            storage.set(key_abx, &vec![2u8]).unwrap();
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

            storage.set(key_abc, &vec![3u8]).unwrap();
            storage.set(key_abx, &vec![4u8]).unwrap();
            commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        storage.checkout(&commit1);
        assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![2u8]);
        // this set be wiped by checkout
        storage.set(key_abc, &vec![8u8]).unwrap();

        storage.checkout(&commit2);
        assert_eq!(storage.get(&key_abc).unwrap(), vec![3u8]);
        assert_eq!(storage.get(&key_abx).unwrap(), vec![4u8]);
    }

    #[test]
    #[serial]
    fn test_persistence_over_reopens() {
        { clean_db(); }

        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let commit1;
        let mut storage = get_storage();
        {
            let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
            storage.set(key_abc, &vec![2 as u8]).unwrap();
            storage.set(key_abx, &vec![3 as u8]).unwrap();
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        assert_eq!(vec![2 as u8], storage.get_history(&commit1, &key_abc).unwrap());
    }


    #[test]
    #[serial]
    fn test_get_errors() {
        { clean_db(); }

        let mut storage = get_storage();

        let res = storage.get(&vec![]);
        assert!(if let MerkleError::KeyEmpty = res.err().unwrap() { true } else { false });

        let res = storage.get(&vec!["a".to_string()]);
        assert!(if let MerkleError::ValueNotFound { .. } = res.err().unwrap() { true } else { false });
    }


    // Test getting entire tree in string format for JSON RPC
    #[test]
    fn test_get_context_tree_by_prefix() {
        let db_name = "ms_test_get_context_tree_by_prefix";
        { clean_db(); }

        let all_json = serde_json::json!(
            {
                "adata": {
                    "b": {
                            "x": {
                                    "y":"090a"
                            }
                    }
                },
                "data": {
                    "a": {
                            "x": {
                                    "y":"0506"
                            }
                    },
                    "b": {
                            "x": {
                                    "y":"0708"
                            }
                    },
                    "c":"0102"
                }
            }
        );
        let data_json = serde_json::json!(
            {
                "a": {
                        "x": {
                                "y":"0506"
                        }
                },
                "b": {
                        "x": {
                                "y":"0708"
                        }
                },
                "c":"0102"
            }
        );

        let mut storage = get_storage();
        let _commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string());

        storage.set(&vec!["data".to_string(), "a".to_string(), "x".to_string()], &vec![3, 4]);
        storage.set(&vec!["data".to_string(), "a".to_string()], &vec![1, 2]);
        storage.set(&vec!["data".to_string(), "a".to_string(), "x".to_string(), "y".to_string()], &vec![5, 6]);
        storage.set(&vec!["data".to_string(), "b".to_string(), "x".to_string(), "y".to_string()], &vec![7, 8]);
        storage.set(&vec!["data".to_string(), "c".to_string()], &vec![1, 2]);
        storage.set(&vec!["adata".to_string(), "b".to_string(), "x".to_string(), "y".to_string()], &vec![9, 10]);
        //data-a[1,2]
        //data-a-x[3,4]
        //data-a-x-y[5,6]
        //data-b-x-y[7,8]
        //data-c[1,2]
        //adata-b-x-y[9,10]
        let commit = storage.commit(0, "Tezos".to_string(), "Genesis".to_string());

        // without depth
        let rv_all = storage.get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec![], None).unwrap();
        assert_json_eq!(all_json, serde_json::to_value(&rv_all).unwrap());

        let rv_data = storage.get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec!["data".to_string()], None).unwrap();
        assert_json_eq!(data_json, serde_json::to_value(&rv_data).unwrap());

        // with depth 0
        assert_json_eq!(
            serde_json::json!(
                null
            ),
            serde_json::to_value(
                storage.get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec![], Some(0)).unwrap()
            ).unwrap()
        );

        // with depth 1
        assert_json_eq!(
            serde_json::json!(
                {
                    "adata": null,
                    "data": null
                }
            ),
            serde_json::to_value(
                storage.get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec![], Some(1)).unwrap()
            ).unwrap()
        );
        // with depth 2
        assert_json_eq!(
            serde_json::json!(
                {
                    "adata": {
                        "b" : null
                    },
                    "data": {
                        "a" : null,
                        "b" : null,
                        "c" : null,
                    },
                }
            ),
            serde_json::to_value(
                storage.get_context_tree_by_prefix(&commit.as_ref().unwrap(), &vec![], Some(2)).unwrap()
            ).unwrap()
        );
    }

    #[test]
    #[serial]
    fn gc_test() {
        clean_db();

        let commit1;
        let commit2;
        let key_abc: &ContextKey = &vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let key_abx: &ContextKey = &vec!["a".to_string(), "b".to_string(), "x".to_string()];
        let key_eab: &ContextKey = &vec!["e".to_string(), "a".to_string(), "b".to_string()];
        let key_az: &ContextKey = &vec!["a".to_string(), "z".to_string()];
        let key_d: &ContextKey = &vec!["d".to_string()];

        let mut storage = get_storage();

        {
            storage.set(key_abc, &vec![1u8, 2u8]);
            storage.set(key_abx, &vec![3u8]);
            assert_eq!(storage.get(&key_abc).unwrap(), vec![1u8, 2u8]);
            assert_eq!(storage.get(&key_abx).unwrap(), vec![3u8]);
            commit1 = storage.commit(0, "".to_string(), "".to_string()).unwrap();

            storage.set(key_az, &vec![4u8]);
            storage.set(key_abx, &vec![5u8]);
            storage.set(key_d, &vec![6u8]);
            storage.set(key_eab, &vec![7u8]);
            assert_eq!(storage.get(key_abx).unwrap(), vec![5u8]);
            commit2 = storage.commit(0, "".to_string(), "".to_string()).unwrap();
        }

        let keys_before_gc = storage.get_merkle_stats().unwrap().db_stats.keys;
        println!("Merkle Stat Before GC: {:#?}", storage.get_merkle_stats());
        assert_eq!(storage.get_history(&commit1, key_abc).unwrap(), vec![1u8, 2u8]);
        assert_eq!(storage.get_history(&commit1, key_abx).unwrap(), vec![3u8]);
        assert_eq!(storage.get_history(&commit2, key_abx).unwrap(), vec![5u8]);
        assert_eq!(storage.get_history(&commit2, key_az).unwrap(), vec![4u8]);
        assert_eq!(storage.get_history(&commit2, key_d).unwrap(), vec![6u8]);
        assert_eq!(storage.get_history(&commit2, key_eab).unwrap(), vec![7u8]);

        storage.gc();
        println!("Merkle Stat After GC: {:#?}", storage.get_merkle_stats());
        let keys_after_gc = storage.get_merkle_stats().unwrap().db_stats.keys;
        assert!(keys_after_gc <= keys_before_gc)
    }
}