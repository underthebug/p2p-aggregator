use rusqlite::Connection;
use ed25519_dalek::{SigningKey, Signer};
use rand::rngs::OsRng;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    // Parse arguments
    let port = args.iter().position(|r| r == "--port").and_then(|i| args.get(i + 1)).unwrap_or(&"41871".to_string()).clone();
    let title = args.iter().position(|r| r == "--title").and_then(|i| args.get(i + 1)).unwrap_or(&"Untitled Broadcast".to_string()).clone();
    let summary = args.iter().position(|r| r == "--summary").and_then(|i| args.get(i + 1)).unwrap_or(&"No message body provided.".to_string()).clone();
    let magnet = args.iter().position(|r| r == "--magnet").and_then(|i| args.get(i + 1)).cloned();
    let region = args.iter().position(|r| r == "--region").and_then(|i| args.get(i + 1))
        .or_else(|| args.iter().position(|r| r == "--regain").and_then(|i| args.get(i + 1)))
        .unwrap_or(&"#global".to_string()).clone();
    let sender_type = args.iter().position(|r| r == "--type").and_then(|i| args.get(i + 1)).unwrap_or(&"human".to_string()).clone();

    let db_path = format!("./data/state_{}.db", port);
    println!("🔍 [p2p-cli] Connected to Node on Port {} (Database: {})", port, db_path);
    println!("📻 [Radio Region]: {} | 👤 [Sender Type]: {}", region, sender_type);
    println!("────────────────────────────────────────────────────────────");

    let conn = Connection::open(&db_path)?;

    // Ensure secure broadcast tables exist with 'region' column
    conn.execute(
        "CREATE TABLE IF NOT EXISTS secure_broadcasts (
            id TEXT PRIMARY KEY,
            region TEXT,
            sender_did TEXT,
            sender_type TEXT,
            content TEXT,
            magnet_link TEXT,
            signature_hex TEXT,
            timestamp INTEGER
        )",
        [],
    )?;
    // Graceful migration if old table used 'regain'
    let _ = conn.execute("ALTER TABLE secure_broadcasts RENAME COLUMN regain TO region", []);

    // Generate sovereign Ed25519 signing key for DIDs
    let mut csprng = OsRng;
    let signing_key: SigningKey = SigningKey::generate(&mut csprng);
    let verifying_key = signing_key.verifying_key();
    let sender_did = format!("did:p2p:{}", hex::encode(verifying_key.to_bytes()));

    let id = uuid::Uuid::new_v4().to_string();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    // Construct payload bytes to sign
    let payload_data = format!("{}:{}:{}:{}:{}:{:?}", id, region, sender_did, summary, timestamp, magnet);
    let signature = signing_key.sign(payload_data.as_bytes());
    let signature_hex = hex::encode(signature.to_bytes());

    // Insert signed broadcast into SQLite
    conn.execute(
        "INSERT INTO secure_broadcasts (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            id,
            region,
            sender_did,
            sender_type,
            summary,
            magnet,
            signature_hex,
            timestamp
        ],
    )?;

    println!("🚀 [p2p-cli] Cryptographically signed broadcast published!");
    println!("  🆔 ID:     {}", id);
    println!("  🔑 DID:    {}", sender_did);
    println!("  ✍️ Sig:    {}...", &signature_hex[..24]);
    println!("  📌 Title:  {}", title);
    if let Some(ref m) = magnet {
        println!("  🧲 Magnet: {}", m);
    }

    Ok(())
}
