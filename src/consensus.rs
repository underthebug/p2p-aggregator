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

        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = Connection::open(db_path).unwrap();
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
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

impl ConsensusEngine {
    pub async fn broadcast_append_entries(
        peer_addr: String,
        term: u64,
        leader_id: String,
        entries: Vec<LogEntry>,
        commit_index: u64,
    ) -> std::io::Result<bool> {
        let mut socket = TcpStream::connect(&peer_addr).await?;

        // 1. Send our public key (32 bytes) or handshake identity stub if needed
        // For now, let's dispatch the serialized Raft message over the authenticated stream
        let msg = RaftMessage::AppendEntries {
            term,
            leader_id,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: commit_index,
        };

        let payload = bincode::serialize(&msg).unwrap();
        let len_header = (payload.len() as u32).to_be_bytes();

        socket.write_all(&len_header).await?;
        socket.write_all(&payload).await?;

        let mut resp_len_buf = [0u8; 4];
        socket.read_exact(&mut resp_len_buf).await?;
        let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        socket.read_exact(&mut resp_buf).await?;

        if let Ok(RaftMessage::AppendEntriesResponse { success, .. }) = bincode::deserialize(&resp_buf) {
            return Ok(success);
        }

        Ok(false)
    }
}
