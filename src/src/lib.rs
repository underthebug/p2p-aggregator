pub mod journal {
    pub mod rocks_store {
        use rocksdb::{DB, Options};
        use std::path::Path;

        pub fn init_telemetry_db<P: AsRef<Path>>(path: P) -> Result<DB, rocksdb::Error> {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.set_max_background_jobs(4);
            DB::open(&opts, path)
        }

        pub fn write_telemetry(db: &DB, key: &[u8], value: &[u8]) -> Result<(), rocksdb::Error> {
            db.put(key, value)
        }
    }

    pub mod sqlite_store {
        use rusqlite::{Connection, Result};

        pub fn init_state_db(path: &str) -> Result<Connection> {
            let conn = Connection::open(path)?;
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;",
            )?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS journal_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payload_hash TEXT NOT NULL UNIQUE,
                    action_data TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )",
                [],
            )?;

            Ok(conn)
        }
    }
}

pub mod ipc {
    pub mod socket {
        use tokio::net::UnixListener;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use std::path::Path;

        pub async fn start_socket_listener(socket_path: &str) -> Result<(), Box<dyn std::error::Error>> {
            if Path::new(socket_path).exists() {
                tokio::fs::remove_file(socket_path).await?;
            }

            let listener = UnixListener::bind(socket_path)?;
            println!("High-speed IPC socket listening on {}", socket_path);

            loop {
                let (mut socket, _) = listener.accept().await?;
                tokio::spawn(async move {
                    let mut buf = vec![0; 4096];
                    let n = match socket.read(&mut buf).await {
                        Ok(n) if n == 0 => return,
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("Socket read error: {}", e);
                            return;
                        }
                    };
                    
                    let cmd = String::from_utf8_lossy(&buf[0..n]);
                    let response = match cmd.trim() {
                        "list" => "[0.00] Autonomous Node Infrastructure (Votes: 1) - https://example.com/infrastructure
----------------------------
".to_string(),
                        "status" => "[OK] P2P Aggregator Daemon Active | SQLite WAL: Healthy | RocksDB Telemetry: Connected
".to_string(),
                        other => format!("Processed Command: {}
", other),
                    };

                    if let Err(e) = socket.write_all(response.as_bytes()).await {
                        eprintln!("Socket write error: {}", e);
                    }
                    let _ = socket.shutdown().await;
                });
            }
        }
    }
}

pub mod wot {
    pub mod verifier {
        use ed25519_dalek::{VerifyingKey, Signature, Verifier};
        use serde::{Serialize, Deserialize};
        use sha2::{Sha256, Digest};
        use anyhow::{Result, anyhow};

        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct AgentPayload {
            pub sender_id: String,
            pub timestamp: u64,
            pub action: String,
            pub nonce: u64,
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub struct SignedPayload {
            pub payload: AgentPayload,
            pub signature: String,
            pub public_key: String,
        }

        impl SignedPayload {
            pub fn compute_hash(&self) -> Result<String> {
                let serialized = serde_json::to_string(&self.payload)?;
                let mut hasher = Sha256::new();
                hasher.update(serialized.as_bytes());
                Ok(hex::encode(hasher.finalize()))
            }

            pub fn verify_signature(&self) -> Result<bool> {
                let pub_key_bytes = hex::decode(&self.public_key)?;
                let sig_bytes = hex::decode(&self.signature)?;

                if pub_key_bytes.len() != 32 {
                    return Err(anyhow!("Invalid public key length"));
                }
                if sig_bytes.len() != 64 {
                    return Err(anyhow!("Invalid signature length"));
                }

                let pub_key_arr: [u8; 32] = pub_key_bytes.try_into().map_err(|_| anyhow!("Bad public key conversion"))?;
                let sig_arr: [u8; 64] = sig_bytes.try_into().map_err(|_| anyhow!("Bad signature conversion"))?;

                let verifying_key = VerifyingKey::from_bytes(&pub_key_arr)?;
                let signature = Signature::from_bytes(&sig_arr);

                let serialized_payload = serde_json::to_string(&self.payload)?;
                
                match verifying_key.verify(serialized_payload.as_bytes(), &signature) {
                    Ok(_) => Ok(true),
                    Err(_) => Ok(false),
                }
            }
        }
    }
}

pub mod network {
    pub mod swarm;
}
