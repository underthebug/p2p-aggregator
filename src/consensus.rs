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

    pub async fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: String,
        entries: Vec<LogEntry>,
    ) -> RaftMessage {
        if term < self.current_term {
            return RaftMessage::AppendEntriesResponse {
                term: self.current_term,
                success: false,
                match_index: 0,
            };
        }

        if term > self.current_term {
            self.current_term = term;
            self.role = NodeRole::Follower;
            self.voted_for = None;
        }

        println!("[Consensus] Accepted AppendEntries from leader {} for term {}", leader_id, term);

        // Store entries in SQLite WAL log
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(db_path).unwrap();
            let tx = conn.transaction().unwrap();
            for entry in entries {
                tx.execute(
                    "INSERT OR REPLACE INTO raft_log (entry_index, term, data) VALUES (?1, ?2, ?3)",
                    rusqlite::params![entry.index as i64, entry.term as i64, entry.data],
                ).unwrap();
            }
            tx.commit().unwrap();
        }).await.unwrap();

        RaftMessage::AppendEntriesResponse {
            term: self.current_term,
            success: true,
            match_index: self.commit_index,
        }
    }
}
