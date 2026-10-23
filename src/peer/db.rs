use rusqlite::{Connection, Result};
use std::path::Path;

pub struct PeerStore {
    conn: Connection,
}

impl PeerStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS peers (
                address TEXT PRIMARY KEY,
                last_seen INTEGER NOT NULL
            )",
            [],
        )?;
        // Enable WAL mode for high-concurrency robust persistence
        conn.execute("PRAGMA journal_mode=WAL;", [])?;
        Ok(Self { conn })
    }

    pub fn save_peer(&self, address: &str) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.conn.execute(
            "INSERT OR REPLACE INTO peers (address, last_seen) VALUES (?1, ?2)",
            rusqlite::params![address, now],
        )?;
        Ok(())
    }

    pub fn load_peers(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare("SELECT address FROM peers")?;
        let peer_iter = stmt.query_map([], |row| row.get(0))?;
        let mut peers = Vec::new();
        for peer in peer_iter {
            if let Ok(addr) = peer {
                peers.push(addr);
            }
        }
        Ok(peers)
    }
}
