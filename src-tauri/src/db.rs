use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use rusqlite::{params, Connection, OptionalExtension, Result};
use sha2::{Digest, Sha512};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecureBroadcast {
    pub id: String,
    pub region: String,
    pub sender_did: String,
    pub sender_type: String,
    pub content: String,
    pub magnet_link: Option<String>,
    #[serde(default)]
    pub sender_public_key_hex: String,
    pub signature_hex: String,
    pub timestamp: i64,
    #[serde(default)]
    pub ai_tag: Option<String>,
}

pub struct DbManager {
    conn: Arc<Mutex<Connection>>,
    signing_key: SigningKey,
}

/// A mesh card the keyword filter matched, with the AI's headline and reason.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RelevantCard {
    pub card_id: String,
    pub matched_keyword: String,
    pub headline: String,
    pub reason: String,
    pub ai_ok: bool,
    pub created_at: i64,
    pub content: String,
    pub region: String,
    pub sender_did: String,
    pub timestamp: i64,
}

impl DbManager {
    pub fn new<P: AsRef<Path>>(path: P) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let path = path.as_ref();
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
        crate::migrations::migrate(&conn)?;
        let signing_key = if path == Path::new(":memory:") {
            SigningKey::generate(&mut OsRng)
        } else {
            load_or_create_signing_key(&path.with_extension("key"))?
        };
        Ok(Self { conn: Arc::new(Mutex::new(conn)), signing_key })
    }

    pub fn sign_message(&self, message: &[u8]) -> [u8; 64] {
        self.signing_key.sign(message).to_bytes()
    }

    pub fn encryption_secret(&self) -> [u8; 32] {
        let expanded = Sha512::digest(self.signing_key.to_bytes());
        let mut secret = [0u8; 32];
        secret.copy_from_slice(&expanded[..32]);
        secret
    }

    pub fn upsert_broadcast(&self, b: &SecureBroadcast) -> Result<()> {
        if !verify_broadcast(b) {
            return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(
                io::Error::new(io::ErrorKind::InvalidData, "broadcast signature is invalid"),
            )));
        }
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO secure_broadcasts
             (id, region, sender_did, sender_type, content, magnet_link, sender_public_key_hex, signature_hex, timestamp, ai_tag)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                b.id,
                b.region,
                b.sender_did,
                b.sender_type,
                b.content,
                b.magnet_link,
                b.sender_public_key_hex,
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
            "SELECT id, region, sender_did, sender_type, content, magnet_link, sender_public_key_hex, signature_hex, timestamp, ai_tag
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
                sender_public_key_hex: row.get(6)?,
                signature_hex: row.get(7)?,
                timestamp: row.get(8)?,
                ai_tag: row.get(9)?,
            })
        })?;

        let mut broadcasts = Vec::new();
        for b in broadcast_iter {
            broadcasts.push(b?);
        }
        Ok(broadcasts)
    }

    pub fn insert_broadcast(&self, region: &str, sender_did: &str, sender_type: &str, content: &str, magnet_link: Option<&str>, ai_tag: Option<&str>) -> Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let id = format!("bc_{}_{}", timestamp, std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().subsec_nanos());
        let broadcast = self.create_signed_broadcast(
            &id, region, sender_did, sender_type, content, magnet_link, ai_tag, timestamp,
        );
        self.insert_local_broadcast(&broadcast)?;
        Ok(())
    }

    pub fn create_signed_broadcast(
        &self,
        id: &str,
        region: &str,
        sender_did: &str,
        sender_type: &str,
        content: &str,
        magnet_link: Option<&str>,
        ai_tag: Option<&str>,
        timestamp: i64,
    ) -> SecureBroadcast {
        let mut broadcast = SecureBroadcast {
            id: id.to_string(),
            region: region.to_string(),
            sender_did: sender_did.to_string(),
            sender_type: sender_type.to_string(),
            content: content.to_string(),
            magnet_link: magnet_link.map(str::to_string),
            sender_public_key_hex: hex::encode(self.signing_key.verifying_key().to_bytes()),
            signature_hex: String::new(),
            timestamp,
            ai_tag: ai_tag.map(str::to_string),
        };
        broadcast.signature_hex = hex::encode(self.signing_key.sign(&broadcast_signing_bytes(&broadcast)).to_bytes());
        broadcast
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
        let broadcast = self.create_signed_broadcast(
            id, region, sender_did, sender_type, content, magnet_link, ai_tag, timestamp,
        );
        let conn = self.conn.lock().unwrap();
        let inserted = insert_broadcast_row(&conn, &broadcast)?;
        Ok(inserted > 0)
    }

    fn insert_local_broadcast(&self, broadcast: &SecureBroadcast) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        insert_broadcast_row(&conn, broadcast)?;
        Ok(())
    }

    pub fn get_broadcast_by_id(&self, id: &str) -> Result<Option<SecureBroadcast>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, region, sender_did, sender_type, content, magnet_link, sender_public_key_hex, signature_hex, timestamp, ai_tag
             FROM secure_broadcasts WHERE id = ?1",
            [id],
            |row| {
                Ok(SecureBroadcast {
                    id: row.get(0)?,
                    region: row.get(1)?,
                    sender_did: row.get(2)?,
                    sender_type: row.get(3)?,
                    content: row.get(4)?,
                    magnet_link: row.get(5)?,
                    sender_public_key_hex: row.get(6)?,
                    signature_hex: row.get(7)?,
                    timestamp: row.get(8)?,
                    ai_tag: row.get(9)?,
                })
            },
        )
        .optional()
    }

    /// None when the card is not listed yet; otherwise whether the AI labelled it.
    pub fn relevant_card_ai_state(&self, card_id: &str) -> Result<Option<bool>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT ai_ok FROM relevant_cards WHERE card_id = ?1", [card_id], |row| row.get(0))
            .optional()
    }

    pub fn insert_relevant_card(&self, card_id: &str, keyword: &str, headline: &str, reason: &str, ai_ok: bool) -> Result<()> {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO relevant_cards (card_id, matched_keyword, headline, reason, ai_ok, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![card_id, keyword, headline, reason, ai_ok, created_at],
        )?;
        Ok(())
    }

    pub fn list_relevant_cards(&self, limit: i64) -> Result<Vec<RelevantCard>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT r.card_id, r.matched_keyword, r.headline, r.reason, r.ai_ok, r.created_at,
                    b.content, b.region, b.sender_did, b.timestamp
             FROM relevant_cards r JOIN secure_broadcasts b ON b.id = r.card_id
             ORDER BY r.created_at DESC, r.rowid DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            Ok(RelevantCard {
                card_id: row.get(0)?,
                matched_keyword: row.get(1)?,
                headline: row.get(2)?,
                reason: row.get(3)?,
                ai_ok: row.get(4)?,
                created_at: row.get(5)?,
                content: row.get(6)?,
                region: row.get(7)?,
                sender_did: row.get(8)?,
                timestamp: row.get(9)?,
            })
        })?;
        rows.collect()
    }

    pub fn local_sender_id(&self) -> String {
        format!("did:ed25519:{}", hex::encode(self.signing_key.verifying_key().to_bytes()))
    }

    pub fn list_subscriptions(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT url FROM rss_subscriptions ORDER BY added_at, url")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut urls = Vec::new();
        for url in rows {
            urls.push(url?);
        }
        Ok(urls)
    }

    pub fn add_subscription(&self, url: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let added_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        conn.execute(
            "INSERT OR IGNORE INTO rss_subscriptions (url, added_at) VALUES (?1, ?2)",
            params![url, added_at],
        )?;
        Ok(())
    }

    pub fn remove_subscription(&self, url: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM rss_subscriptions WHERE url = ?1", params![url])?;
        Ok(())
    }

}

fn broadcast_signing_bytes(broadcast: &SecureBroadcast) -> Vec<u8> {
    serde_json::to_vec(&(
        broadcast.id.as_str(),
        broadcast.region.as_str(),
        broadcast.sender_did.as_str(),
        broadcast.sender_type.as_str(),
        broadcast.content.as_str(),
        broadcast.magnet_link.as_deref(),
        broadcast.timestamp,
        broadcast.ai_tag.as_deref(),
    ))
    .expect("broadcast signing fields are serializable")
}

fn verify_broadcast(broadcast: &SecureBroadcast) -> bool {
    let public_key_hex = if broadcast.sender_public_key_hex.is_empty() {
        broadcast
            .sender_did
            .strip_prefix("did:ed25519:")
            .or_else(|| broadcast.sender_did.strip_prefix("did:p2p:"))
    } else {
        Some(broadcast.sender_public_key_hex.as_str())
    };
    let Some(public_key_hex) = public_key_hex else { return false };
    let Ok(public_bytes) = hex::decode(public_key_hex) else { return false };
    let Ok(signature_bytes) = hex::decode(&broadcast.signature_hex) else { return false };
    let Ok(public_bytes) = <[u8; 32]>::try_from(public_bytes) else { return false };
    let Ok(signature_bytes) = <[u8; 64]>::try_from(signature_bytes) else { return false };
    let expected_key = hex::encode(public_bytes);
    let expected_did = format!("did:ed25519:{expected_key}");
    let legacy_did = format!("did:p2p:{expected_key}");
    if broadcast.sender_did != expected_did && broadcast.sender_did != legacy_did {
        return false;
    }
    let Ok(public_key) = VerifyingKey::from_bytes(&public_bytes) else { return false };
    let signature = Signature::from_bytes(&signature_bytes);
    public_key.verify(&broadcast_signing_bytes(broadcast), &signature).is_ok()
}

fn insert_broadcast_row(conn: &Connection, broadcast: &SecureBroadcast) -> Result<usize> {
    conn.execute(
        "INSERT OR IGNORE INTO secure_broadcasts
         (id, region, sender_did, sender_type, content, magnet_link, sender_public_key_hex, signature_hex, timestamp, ai_tag)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            broadcast.id,
            broadcast.region,
            broadcast.sender_did,
            broadcast.sender_type,
            broadcast.content,
            broadcast.magnet_link,
            broadcast.sender_public_key_hex,
            broadcast.signature_hex,
            broadcast.timestamp,
            broadcast.ai_tag,
        ],
    )
}

fn load_or_create_signing_key(path: &Path) -> io::Result<SigningKey> {
    match fs::read(path) {
        Ok(bytes) => signing_key_from_bytes(bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
                fs::create_dir_all(parent)?;
            }
            let key = SigningKey::generate(&mut OsRng);
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(path) {
                Ok(mut file) => {
                    file.write_all(&key.to_bytes())?;
                    Ok(key)
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    signing_key_from_bytes(fs::read(path)?)
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

fn signing_key_from_bytes(bytes: Vec<u8>) -> io::Result<SigningKey> {
    let seed: [u8; 32] = bytes
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "identity key must be exactly 32 bytes"))?;
    Ok(SigningKey::from_bytes(&seed))
}

#[cfg(test)]
mod tests {
    use super::{verify_broadcast, DbManager};
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

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

    #[test]
    fn signed_broadcasts_verify_and_tampering_is_rejected() {
        let db = DbManager::new(":memory:").unwrap();
        let sender = db.local_sender_id();
        db.insert_broadcast_with_id(
            "signed-1",
            "#global",
            &sender,
            "node",
            "authentic content",
            None,
            Some("#test"),
            123,
        )
        .unwrap();
        let broadcast = db.get_broadcast_by_id("signed-1").unwrap().unwrap();
        assert_ne!(broadcast.signature_hex, "local_sig_ok");
        assert!(verify_broadcast(&broadcast));
        db.upsert_broadcast(&broadcast).unwrap();

        let mut tampered = broadcast;
        tampered.content.push_str(" changed");
        assert!(db.upsert_broadcast(&tampered).is_err());
    }

    #[test]
    fn root_wire_envelope_verifies_without_tauri_only_fields() {
        let db = DbManager::new(":memory:").unwrap();
        let signing_key = SigningKey::generate(&mut OsRng);
        let public_key_hex = hex::encode(signing_key.verifying_key().to_bytes());
        let sender_did = format!("did:ed25519:{public_key_hex}");
        let signing_bytes = serde_json::to_vec(&(
            "root-1",
            "#global",
            sender_did.as_str(),
            "human",
            "shared envelope",
            None::<&str>,
            123_i64,
            None::<&str>,
        ))
        .unwrap();
        let signature_hex = hex::encode(signing_key.sign(&signing_bytes).to_bytes());
        let envelope = serde_json::json!({
            "id": "root-1",
            "region": "#global",
            "sender_did": sender_did,
            "sender_type": "human",
            "content": "shared envelope",
            "magnet_link": null,
            "signature_hex": signature_hex,
            "timestamp": 123,
            "ai_tag": null
        });
        let broadcast: super::SecureBroadcast = serde_json::from_value(envelope).unwrap();

        assert!(broadcast.sender_public_key_hex.is_empty());
        db.upsert_broadcast(&broadcast).unwrap();
        assert_eq!(db.get_broadcasts(10).unwrap().len(), 1);
    }

    #[test]
    fn legacy_secure_broadcasts_table_gains_ai_tag_column() {
        let path = std::env::temp_dir().join(format!(
            "legacy_broadcasts_{}_{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE secure_broadcasts (
                    id TEXT PRIMARY KEY, region TEXT NOT NULL, sender_did TEXT NOT NULL,
                    sender_type TEXT NOT NULL, content TEXT NOT NULL, magnet_link TEXT,
                    signature_hex TEXT NOT NULL, timestamp INTEGER NOT NULL
                );",
            )
            .unwrap();
        }

        let db = DbManager::new(&path).unwrap();
        db.insert_broadcast("#global", "did:p2p:rss-bot", "node", "📰 headline", None, Some("#rss"))
            .unwrap();
        let rows = db.get_broadcasts(10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ai_tag.as_deref(), Some("#rss"));

        drop(db);
        let _ = std::fs::remove_file(&path);
    }
}
