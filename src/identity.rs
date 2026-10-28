use rusqlite::{Connection, Result};
use ed25519_dalek::{SigningKey, VerifyingKey, Signature, Signer, Verifier};

#[derive(Debug, Clone)]
pub struct IdentityRecord {
    pub did: String,
    pub alias: String,
    pub public_key_hex: String,
    pub trust_score: i32,
}

#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    pub id: String,
    pub regain: String,
    pub sender_did: String,
    pub sender_type: String, // "human" or "ai"
    pub content: String,
    pub magnet_link: Option<String>,
    pub signature_hex: String,
    pub timestamp: i64,
}

pub fn init_identity_db(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS identities (
            did TEXT PRIMARY KEY,
            alias TEXT,
            public_key_hex TEXT,
            trust_score INTEGER DEFAULT 0
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS endorsements (
            issuer_did TEXT,
            target_did TEXT,
            weight INTEGER,
            signature TEXT,
            PRIMARY KEY (issuer_did, target_did)
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS secure_broadcasts (
            id TEXT PRIMARY KEY,
            regain TEXT,
            sender_did TEXT,
            sender_type TEXT,
            content TEXT,
            magnet_link TEXT,
            signature_hex TEXT,
            timestamp INTEGER
        )",
        [],
    )?;

    Ok(())
}

pub fn sign_payload(signing_key: &SigningKey, payload: &[u8]) -> String {
    let signature: Signature = signing_key.sign(payload);
    hex::encode(signature.to_bytes())
}

pub fn verify_signature(public_key_bytes: &[u8], payload: &[u8], sig_hex: &str) -> bool {
    if let Ok(bytes_32) = public_key_bytes.try_into() {
        if let Ok(pub_key) = VerifyingKey::from_bytes(&bytes_32) {
            if let Ok(sig_bytes) = hex::decode(sig_hex) {
                if let Ok(bytes_64) = sig_bytes.try_into() {
                    let signature = Signature::from_bytes(&bytes_64);
                    return pub_key.verify(payload, &signature).is_ok();
                }
            }
        }
    }
    false
}
