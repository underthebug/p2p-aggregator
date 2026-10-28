use p2p_aggregator::journal::sqlite::{ensure_journal_table, record_transaction};

#[test]
fn test_idempotency_constraint_collision() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    ensure_journal_table(&conn).unwrap();

    let hash = "sha256_mock_payload_hash_12345";
    let action = "{\"action\": \"execute_macro\"}";

    assert!(record_transaction(&conn, hash, action, "PENDING").unwrap(), "First insert should be recorded");
    assert!(
        !record_transaction(&conn, hash, action, "PENDING").unwrap(),
        "Duplicate action bypassed crash recovery uniqueness constraint!"
    );
}
