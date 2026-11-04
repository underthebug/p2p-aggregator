use rusqlite::{params, Connection};

pub fn init_state_db(path: &str) -> Result<Connection, Box<dyn std::error::Error>> {
    let conn = Connection::open(path)?;
    ensure_journal_table(&conn)?;
    Ok(conn)
}

pub fn ensure_journal_table(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS journal_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload_hash TEXT NOT NULL UNIQUE,
            action_data TEXT NOT NULL,
            status TEXT NOT NULL
        )",
        [],
    )?;
    Ok(())
}

/// Returns `false` if this payload hash was already journaled.
pub fn record_transaction(conn: &Connection, payload_hash: &str, action_data: &str, status: &str) -> rusqlite::Result<bool> {
    let rows = conn.execute(
        "INSERT OR IGNORE INTO journal_transactions (payload_hash, action_data, status) VALUES (?1, ?2, ?3)",
        params![payload_hash, action_data, status],
    )?;
    Ok(rows == 1)
}

pub fn is_recorded(conn: &Connection, payload_hash: &str) -> rusqlite::Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM journal_transactions WHERE payload_hash = ?1",
        params![payload_hash],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}
