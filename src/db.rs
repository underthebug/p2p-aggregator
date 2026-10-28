use rusqlite::{Connection, Result};

pub struct StateDatabase {
    conn: Connection,
}

impl StateDatabase {
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS feed_cards (
                id TEXT PRIMARY KEY,
                author_peer_id TEXT NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                summary TEXT NOT NULL,
                magnet_link TEXT,
                timestamp INTEGER NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS votes (
                card_id TEXT NOT NULL,
                voter_peer_id TEXT NOT NULL,
                direction INTEGER NOT NULL,
                PRIMARY KEY (card_id, voter_peer_id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_json TEXT NOT NULL,
                status TEXT DEFAULT 'pending'
            )",
            [],
        )?;

        Ok(Self { conn })
    }

    pub fn save_feed_card(
        &mut self,
        id: &str,
        author_peer_id: &str,
        title: &str,
        url: &str,
        summary: &str,
        magnet_link: Option<&str>,
        timestamp: u64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO feed_cards (id, author_peer_id, title, url, summary, magnet_link, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![id, author_peer_id, title, url, summary, magnet_link, timestamp],
        )?;
        Ok(())
    }

    pub fn save_vote(&mut self, card_id: &str, voter_peer_id: &str, direction: i32) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO votes (card_id, voter_peer_id, direction)
             VALUES (?1, ?2, ?3)",
            rusqlite::params![card_id, voter_peer_id, direction],
        )?;
        Ok(())
    }

    pub fn get_pending_outbox(&mut self) -> Result<Vec<(i64, String)>> {
        let mut stmt = self.conn.prepare("SELECT id, msg_json FROM outbox WHERE status = 'pending'")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut items = Vec::new();
        for r in rows {
            items.push(r?);
        }
        Ok(items)
    }

    pub fn mark_outbox_sent(&mut self, id: i64) -> Result<()> {
        self.conn.execute("UPDATE outbox SET status = 'sent' WHERE id = ?1", [id])?;
        Ok(())
    }
}
