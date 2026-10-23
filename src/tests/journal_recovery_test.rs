use rusqlite::params;

#[test]
fn test_idempotency_constraint_collision() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    // Use execute_batch for PRAGMA statements that return result rows
    conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
    
    conn.execute(
        "CREATE TABLE IF NOT EXISTS journal_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload_hash TEXT NOT NULL UNIQUE,
            action_data TEXT NOT NULL,
            status TEXT NOT NULL
        )",
        [],
    ).unwrap();

    let hash = "sha256_mock_payload_hash_12345";

    let insert_first = conn.execute(
        "INSERT INTO journal_transactions (payload_hash, action_data, status) VALUES (?1, ?2, ?3)",
        params![hash, "{\"action\": \"execute_macro\"}", "PENDING"],
    );
    assert!(insert_first.is_ok());

    let insert_replay = conn.execute(
        "INSERT INTO journal_transactions (payload_hash, action_data, status) VALUES (?1, ?2, ?3)",
        params![hash, "{\"action\": \"execute_macro\"}", "PENDING"],
    );

    assert!(insert_replay.is_err(), "Duplicate action bypassed crash recovery uniqueness constraint!");
}
