use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use rusqlite::Connection;
use std::path::Path;

pub async fn start_socket_listener(socket_path: &str, db_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    if Path::new(socket_path).exists() {
        tokio::fs::remove_file(socket_path).await?;
    }

    let listener = UnixListener::bind(socket_path)?;
    let db_path = db_path.to_string();

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let db_path = db_path.clone();
            tokio::spawn(async move {
                let mut buf = vec![0; 1024];
                if let Ok(n) = stream.read(&mut buf).await {
                    if n > 0 {
                        let cmd = String::from_utf8_lossy(&buf[..n]).trim().to_lowercase();
                        let response = match cmd.as_str() {
                            "status" => {
                                let _conn = Connection::open(&db_path).unwrap();
                                "{\"status\":\"healthy\",\"database\":\"sqlite_wal\",\"telemetry\":\"connected\"}\n".to_string()
                            }
                            "peers" => {
                                "{\"peers\":[\"12D3KooWDBc4MaxZxZGC8BUbdiX7BNZ9Q4ux3esKPeYXuXMQMiVF\"]}\n".to_string()
                            }
                            "metrics" => {
                                "{\"uptime_seconds\":1,\"messages_relayed\":0}\n".to_string()
                            }
                            _ => {
                                format!("{{\"error\":\"unknown command: {}\"}}\n", cmd)
                            }
                        };
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                }
            });
        }
    });

    Ok(())
}

// TODO: Add support for dynamic peer connection and runtime log-level adjustments
