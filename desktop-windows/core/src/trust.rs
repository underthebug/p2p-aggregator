#![allow(dead_code)]

use rusqlite::{params, Connection, Result};

pub struct TrustEngine;

impl TrustEngine {
    pub fn set_trust(conn: &Connection, truster: &str, trustee: &str, weight: f64) -> Result<()> {
        let clamped = weight.clamp(-1.0, 1.0);
        conn.execute(
            "INSERT OR REPLACE INTO trust_matrix (truster_pubkey, trustee_pubkey, weight)
             VALUES (?1, ?2, ?3)",
            params![truster, trustee, clamped],
        )?;
        Ok(())
    }

    pub fn calculate_transitive_trust(conn: &Connection, truster: &str, trustee: &str) -> f64 {
        conn.query_row(
            "SELECT weight FROM trust_matrix WHERE truster_pubkey = ?1 AND trustee_pubkey = ?2",
            params![truster, trustee],
            |row| row.get(0),
        )
        .unwrap_or(1.0)
    }
}
