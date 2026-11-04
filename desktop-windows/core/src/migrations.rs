use rusqlite::{Connection, Result, Transaction};

pub const LATEST_SCHEMA_VERSION: i32 = 3;

pub fn migrate(conn: &Connection) -> Result<()> {
    let mut version: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > LATEST_SCHEMA_VERSION {
        return Err(rusqlite::Error::InvalidQuery);
    }
    while version < LATEST_SCHEMA_VERSION {
        let next_version = version + 1;
        let tx = conn.unchecked_transaction()?;
        match next_version {
            1 => migrate_v1(&tx)?,
            2 => migrate_v2(&tx)?,
            3 => migrate_v3(&tx)?,
            _ => unreachable!(),
        }
        tx.pragma_update(None, "user_version", next_version)?;
        tx.commit()?;
        version = next_version;
    }
    Ok(())
}

fn migrate_v1(conn: &Transaction<'_>) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS feed_cards (
            id TEXT PRIMARY KEY,
            author_peer_id TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            summary TEXT NOT NULL,
            magnet_link TEXT,
            timestamp INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS votes (
            card_id TEXT NOT NULL,
            voter_peer_id TEXT NOT NULL,
            direction INTEGER NOT NULL,
            PRIMARY KEY (card_id, voter_peer_id)
        );
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            msg_json TEXT NOT NULL,
            status TEXT DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS journal_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload_hash TEXT NOT NULL UNIQUE,
            action_data TEXT NOT NULL,
            status TEXT NOT NULL
        );",
    )
}

fn migrate_v2(conn: &Transaction<'_>) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS secure_broadcasts (
            id TEXT PRIMARY KEY,
            region TEXT NOT NULL,
            sender_did TEXT NOT NULL,
            sender_type TEXT NOT NULL,
            content TEXT NOT NULL,
            magnet_link TEXT,
            signature_hex TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );",
    )?;
    let has_region = conn
        .prepare("SELECT 1 FROM pragma_table_info('secure_broadcasts') WHERE name = 'region'")?
        .exists([])?;
    let has_regain = conn
        .prepare("SELECT 1 FROM pragma_table_info('secure_broadcasts') WHERE name = 'regain'")?
        .exists([])?;
    if !has_region && has_regain {
        conn.execute("ALTER TABLE secure_broadcasts RENAME COLUMN regain TO region", [])?;
    }
    Ok(())
}

fn migrate_v3(conn: &Transaction<'_>) -> Result<()> {
    let exists = conn
        .prepare("SELECT 1 FROM pragma_table_info('secure_broadcasts') WHERE name = 'ai_tag'")?
        .exists([])?;
    if !exists {
        conn.execute("ALTER TABLE secure_broadcasts ADD COLUMN ai_tag TEXT", [])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{migrate, LATEST_SCHEMA_VERSION};
    use rusqlite::Connection;

    #[test]
    fn migration_is_versioned_and_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        migrate(&conn).unwrap();
        let version: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap();
        assert_eq!(version, LATEST_SCHEMA_VERSION);
        let tables: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('feed_cards', 'votes', 'outbox', 'journal_transactions', 'secure_broadcasts')",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(tables, 5);
    }
}