use rusqlite::{Connection, Result, params};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SecureBroadcast {
    pub id: String,
    pub region: String,
    pub sender_did: String,
    pub sender_type: String,
    pub content: String,
    pub magnet_link: Option<String>,
    pub signature_hex: String,
    pub timestamp: i64,
    pub ai_tag: Option<String>,
}

pub struct DbManager {
    conn: Arc<Mutex<Connection>>,
}

impl DbManager {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        
        conn.execute_batch("
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            
            CREATE TABLE IF NOT EXISTS secure_broadcasts (
                id TEXT PRIMARY KEY,
                region TEXT NOT NULL,
                sender_did TEXT NOT NULL,
                sender_type TEXT NOT NULL,
                content TEXT NOT NULL,
                magnet_link TEXT,
                signature_hex TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                ai_tag TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_broadcasts_timestamp ON secure_broadcasts(timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_broadcasts_region ON secure_broadcasts(region);
        ")?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn upsert_broadcast(&self, b: &SecureBroadcast) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO secure_broadcasts 
             (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp, ai_tag) 
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                b.id,
                b.region,
                b.sender_did,
                b.sender_type,
                b.content,
                b.magnet_link,
                b.signature_hex,
                b.timestamp,
                b.ai_tag
            ],
        )?;
        Ok(())
    }

    pub fn get_broadcasts(&self, limit: i64) -> Result<Vec<SecureBroadcast>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp, ai_tag 
             FROM secure_broadcasts ORDER BY timestamp DESC LIMIT ?1"
        )?;

        let broadcast_iter = stmt.query_map(params![limit], |row| {
            Ok(SecureBroadcast {
                id: row.get(0)?,
                region: row.get(1)?,
                sender_did: row.get(2)?,
                sender_type: row.get(3)?,
                content: row.get(4)?,
                magnet_link: row.get(5)?,
                signature_hex: row.get(6)?,
                timestamp: row.get(7)?,
                ai_tag: row.get(8)?,
            })
        })?;

        let mut broadcasts = Vec::new();
        for b in broadcast_iter {
            broadcasts.push(b?);
        }
        Ok(broadcasts)
    }

    pub fn insert_broadcast(&self, region: &str, sender_did: &str, sender_type: &str, content: &str, magnet_link: Option<&str>, ai_tag: Option<&str>) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let id = format!("bc_{}_{}", timestamp, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos());

        conn.execute(
            "INSERT INTO secure_broadcasts (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp, ai_tag) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                id,
                region,
                sender_did,
                sender_type,
                content,
                magnet_link,
                "local_sig_ok",
                timestamp,
                ai_tag
            ],
        )?;
        Ok(())
    }

    pub fn insert_broadcast_with_id(
        &self,
        id: &str,
        region: &str,
        sender_did: &str,
        sender_type: &str,
        content: &str,
        magnet_link: Option<&str>,
        ai_tag: Option<&str>,
        timestamp: i64,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let inserted = conn.execute(
            "INSERT OR IGNORE INTO secure_broadcasts
             (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp, ai_tag)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                id,
                region,
                sender_did,
                sender_type,
                content,
                magnet_link,
                "local_sig_ok",
                timestamp,
                ai_tag
            ],
        )?;
        Ok(inserted > 0)
    }

}

#[cfg(test)]
mod tests {
    use super::DbManager;

    #[test]
    fn broadcast_id_makes_duplicate_insertions_idempotent() {
        let db = DbManager::new(":memory:").unwrap();
        let insert = |id| {
            db.insert_broadcast_with_id(
                id,
                "#global",
                "peer-a",
                "human",
                "hello",
                None,
                None,
                123,
            )
            .unwrap()
        };

        assert!(insert("broadcast-1"));
        assert!(!insert("broadcast-1"));
        assert!(insert("broadcast-2"));
        assert_eq!(db.get_broadcasts(10).unwrap().len(), 2);
    }
}
