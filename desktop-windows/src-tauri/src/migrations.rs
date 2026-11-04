use rusqlite::{Connection, Result, Transaction};

pub const LATEST_SCHEMA_VERSION: i32 = 4;

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
            2 => add_column_if_missing(
                &tx,
                "sender_public_key_hex",
                "ALTER TABLE secure_broadcasts ADD COLUMN sender_public_key_hex TEXT NOT NULL DEFAULT ''",
            )?,
            3 => tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS user_feeds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE
                );",
            )?,
            4 => tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS relevant_cards (
                    card_id TEXT PRIMARY KEY,
                    matched_keyword TEXT NOT NULL,
                    headline TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    ai_ok INTEGER NOT NULL,
                    created_at INTEGER NOT NULL
                );",
            )?,
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
        "CREATE TABLE IF NOT EXISTS secure_broadcasts (
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
        CREATE TABLE IF NOT EXISTS rss_subscriptions (
            url TEXT PRIMARY KEY,
            added_at INTEGER NOT NULL
        );",
    )?;
    add_column_if_missing(conn, "ai_tag", "ALTER TABLE secure_broadcasts ADD COLUMN ai_tag TEXT")
}

fn add_column_if_missing(conn: &Connection, column: &str, alter_sql: &str) -> Result<()> {
    let exists = conn
        .prepare("SELECT 1 FROM pragma_table_info('secure_broadcasts') WHERE name = ?1")?
        .exists([column])?;
    if !exists {
        conn.execute(alter_sql, [])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{migrate, LATEST_SCHEMA_VERSION};
    use rusqlite::Connection;

    #[test]
    fn migration_upgrades_legacy_broadcast_schema() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE secure_broadcasts (
                id TEXT PRIMARY KEY, region TEXT, sender_did TEXT, sender_type TEXT,
                content TEXT, magnet_link TEXT, signature_hex TEXT, timestamp INTEGER
            );
            INSERT INTO secure_broadcasts VALUES ('old', '#global', 'peer', 'node', 'hello', NULL, 'legacy', 1);",
        )
        .unwrap();

        migrate(&conn).unwrap();

        let version: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0)).unwrap();
        assert_eq!(version, LATEST_SCHEMA_VERSION);
        let key_column_exists: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('secure_broadcasts') WHERE name = 'sender_public_key_hex'")
            .unwrap()
            .exists([])
            .unwrap();
        assert!(key_column_exists);
        let preserved: String = conn
            .query_row("SELECT content FROM secure_broadcasts WHERE id = 'old'", [], |row| row.get(0))
            .unwrap();
        assert_eq!(preserved, "hello");
    }
}