use std::sync::Arc;
use tokio::sync::Mutex;
use rusqlite::Connection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

pub struct ConsensusEngine {
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub role: NodeRole,
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

        // Load or initialize term
        let term: u64 = conn.query_row(
            "SELECT value FROM consensus_state WHERE key = 'current_term'",
            [],
            |row| row.get::<_, String>(0).map(|v| v.parse().unwrap_or(0))
        ).unwrap_or(0);

        Arc::new(Mutex::new(Self {
            current_term: term,
            voted_for: None,
            role: NodeRole::Follower,
            db_path: db_path.to_string(),
        }))
    }

    pub async fn persist_term(&self) {
        let db_path = self.db_path.clone();
        let term = self.current_term;
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(db_path).unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO consensus_state (key, value) VALUES ('current_term', ?1)",
                [term.to_string()],
            ).unwrap();
        }).await.unwrap();
    }
}
