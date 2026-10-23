use std::sync::Arc;
use tokio::sync::Mutex;
use rusqlite::Connection;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
}

pub struct ConsensusEngine {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub role: NodeRole,
    pub commit_index: u64,
    pub last_applied: u64,
    db_path: String,
}

impl ConsensusEngine {
    pub fn new(db_path: &str) -> Arc<Mutex<Self>> {
        let conn = Connection::open(db_path).expect("Failed to open consensus db");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS consensus_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        ).unwrap();

        conn.execute(
            "CREATE TABLE IF NOT EXISTS raft_log (
                entry_index INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                data BLOB NOT NULL
            )",
            [],
        ).unwrap();

        let term: u64 = conn.query_row(
            "SELECT value FROM consensus_state WHERE key = 'current_term'",
            [],
            |row| row.get::<_, String>(0).map(|v| v.parse().unwrap_or(0))
        ).unwrap_or(0);

        Arc::new(Mutex::new(Self {
            current_term: term,
            voted_for: None,
            role: NodeRole::Follower,
            commit_index: 0,
            last_applied: 0,
            db_path: db_path.to_string(),
        }))
    }
}
