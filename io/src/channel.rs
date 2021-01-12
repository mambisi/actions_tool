use serde::{Serialize, Deserialize};
use std::cmp::Ordering::Equal;

type Hash = Vec<u8>;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Block {
    pub block_level : u32,
    pub block_hash : String
}

impl Block {
    pub fn new(block_level : u32, block_hash : String) -> Self {
        Block {
            block_level,
            block_hash
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContextActionJson {
    #[serde(flatten)]
    pub action: ContextAction
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ContextAction {
    Set {
        context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        operation_hash: Option<Hash>,
        key: Vec<String>,
        value: Vec<u8>,
        value_as_json: Option<String>,
        ignored: bool,
        start_time: f64,
        end_time: f64,
    },
    Delete {
        context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        operation_hash: Option<Hash>,
        key: Vec<String>,
        ignored: bool,
        start_time: f64,
        end_time: f64,
    },
    RemoveRecursively {
        context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        operation_hash: Option<Hash>,
        key: Vec<String>,
        ignored: bool,
        start_time: f64,
        end_time: f64,
    },
    Copy {
        context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        operation_hash: Option<Hash>,
        from_key: Vec<String>,
        to_key: Vec<String>,
        ignored: bool,
        start_time: f64,
        end_time: f64,
    },
    Checkout {
        context_hash: Hash,
        start_time: f64,
        end_time: f64,
    },
    Commit {
        parent_context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        new_context_hash: Hash,
        author: String,
        message: String,
        date: i64,
        parents: Vec<Vec<u8>>,
        start_time: f64,
        end_time: f64,
    },
    Mem {
        context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        operation_hash: Option<Hash>,
        key: Vec<String>,
        value: bool,
        start_time: f64,
        end_time: f64,
    },
    DirMem {
        context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        operation_hash: Option<Hash>,
        key: Vec<String>,
        value: bool,
        start_time: f64,
        end_time: f64,
    },
    Get {
        context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        operation_hash: Option<Hash>,
        key: Vec<String>,
        value: Vec<u8>,
        value_as_json: Option<String>,
        start_time: f64,
        end_time: f64,
    },
    Fold {
        context_hash: Option<Hash>,
        block_hash: Option<Hash>,
        operation_hash: Option<Hash>,
        key: Vec<String>,
        start_time: f64,
        end_time: f64,
    },
    /// This is a control event used to shutdown IPC channel
    Shutdown,
}

fn get_time(action: &ContextAction) -> f64 {
    match action {
        ContextAction::Set {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::Delete {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::RemoveRecursively {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::Copy {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::Checkout {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::Commit {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::Mem {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::DirMem {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::Get {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::Fold {
            start_time,
            end_time,
            ..
        } => *end_time - *start_time,
        ContextAction::Shutdown => 0_f64,
    }
}

impl Ord for ContextAction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        get_time(&self)
            .partial_cmp(&get_time(&other))
            .unwrap_or(Equal)
    }
}

impl PartialOrd for ContextAction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for ContextAction {
    fn eq(&self, other: &Self) -> bool {
        get_time(&self) == get_time(&other)
    }
}

impl Eq for ContextAction {}