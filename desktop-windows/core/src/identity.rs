use rusqlite::{Connection, Result};
use ed25519_dalek::{SigningKey, VerifyingKey, Signature, Signer, Verifier};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct IdentityRecord {
    pub did: String,
    pub alias: String,
    pub public_key_hex: String,
    pub trust_score: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastMessage {
    pub id: String,
    pub region: String,
    pub sender_did: String,
    pub sender_type: String, // "human" or "ai"
    pub content: String,
    pub magnet_link: Option<String>,
    #[serde(default)]
    pub ai_tag: Option<String>,
    pub signature_hex: String,
    pub timestamp: i64,
}

pub fn init_identity_db(conn: &Connection) -> Result<()> {
    crate::migrations::migrate(conn)?;
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

    Ok(())
}

pub fn sign_payload(signing_key: &SigningKey, payload: &[u8]) -> String {
    let signature: Signature = signing_key.sign(payload);
    hex::encode(signature.to_bytes())
}

pub fn broadcast_signing_bytes(
    id: &str,
    region: &str,
    sender_did: &str,
    sender_type: &str,
    content: &str,
    magnet_link: Option<&str>,
    timestamp: i64,
    ai_tag: Option<&str>,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&(id, region, sender_did, sender_type, content, magnet_link, timestamp, ai_tag))
}

pub fn sign_broadcast(
    signing_key: &SigningKey,
    id: &str,
    region: &str,
    sender_did: &str,
    sender_type: &str,
    content: &str,
    magnet_link: Option<&str>,
    timestamp: i64,
    ai_tag: Option<&str>,
) -> Result<String, serde_json::Error> {
    let payload = broadcast_signing_bytes(id, region, sender_did, sender_type, content, magnet_link, timestamp, ai_tag)?;
    Ok(sign_payload(signing_key, &payload))
}

pub fn verify_broadcast_signature(
    public_key_bytes: &[u8],
    id: &str,
    region: &str,
    sender_did: &str,
    sender_type: &str,
    content: &str,
    magnet_link: Option<&str>,
    timestamp: i64,
    ai_tag: Option<&str>,
    signature_hex: &str,
) -> bool {
    let Ok(payload) = broadcast_signing_bytes(id, region, sender_did, sender_type, content, magnet_link, timestamp, ai_tag) else {
        return false;
    };
    verify_signature(public_key_bytes, &payload, signature_hex)
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

pub fn public_key_from_did(did: &str) -> Option<[u8; 32]> {
    let encoded = did
        .strip_prefix("did:ed25519:")
        .or_else(|| did.strip_prefix("did:p2p:"))?;
    let bytes = hex::decode(encoded).ok()?;
    bytes.try_into().ok()
}

#[cfg(test)]
mod tests {
    use super::{public_key_from_did, sign_broadcast, verify_broadcast_signature};
    use ed25519_dalek::SigningKey;
    use rand::rngs::OsRng;

    #[test]
    fn signed_broadcast_verifies_and_detects_tampering() {
        let signing_key = SigningKey::generate(&mut OsRng);
        let public_key = signing_key.verifying_key().to_bytes();
        let sender_did = format!("did:ed25519:{}", hex::encode(public_key));
        let signature = sign_broadcast(
            &signing_key,
            "id-1",
            "#global",
            &sender_did,
            "human",
            "hello",
            None,
            1,
            None,
        )
        .unwrap();

        assert_eq!(public_key_from_did(&sender_did), Some(public_key));
        assert!(verify_broadcast_signature(
            &public_key, "id-1", "#global", &sender_did, "human", "hello", None, 1, None, &signature,
        ));
        assert!(!verify_broadcast_signature(
            &public_key, "id-1", "#global", &sender_did, "human", "changed", None, 1, None, &signature,
        ));
    }

    #[test]
    fn tauri_style_envelope_verifies_with_shared_fields() {
        use super::{public_key_from_did, verify_broadcast_signature, BroadcastMessage};
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let signing_key = SigningKey::generate(&mut OsRng);
        let public_key = signing_key.verifying_key().to_bytes();
        let public_key_hex = hex::encode(public_key);
        let sender_did = format!("did:ed25519:{public_key_hex}");
        let signature = sign_broadcast(
            &signing_key,
            "tauri-1",
            "#global",
            &sender_did,
            "node",
            "cross-crate",
            None,
            7,
            Some("#rss"),
        )
        .unwrap();
        let value = serde_json::json!({
            "id": "tauri-1",
            "region": "#global",
            "sender_did": sender_did,
            "sender_type": "node",
            "content": "cross-crate",
            "magnet_link": null,
            "sender_public_key_hex": public_key_hex,
            "signature_hex": signature,
            "timestamp": 7,
            "ai_tag": "#rss"
        });
        let broadcast: BroadcastMessage = serde_json::from_value(value).unwrap();
        let key = public_key_from_did(&broadcast.sender_did).unwrap();

        assert!(verify_broadcast_signature(
            &key,
            &broadcast.id,
            &broadcast.region,
            &broadcast.sender_did,
            &broadcast.sender_type,
            &broadcast.content,
            broadcast.magnet_link.as_deref(),
            broadcast.timestamp,
            broadcast.ai_tag.as_deref(),
            &broadcast.signature_hex,
        ));
    }
}
