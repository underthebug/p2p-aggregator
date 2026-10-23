use rusqlite::{Connection, Result};
use std::path::Path;
use std::sync::Mutex;

pub struct PeerStore {
    conn: Mutex<Connection>,
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
        Ok(Self { conn: Mutex::new(conn) })
    }

    pub fn save_peer(&self, address: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        conn.execute(
            "INSERT OR REPLACE INTO peers (address, last_seen) VALUES (?1, ?2)",
            rusqlite::params![address, now],
        )?;
        Ok(())
    }

    pub fn load_peers(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT address FROM peers")?;
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
