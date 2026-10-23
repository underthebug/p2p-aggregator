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
                        let _conn = Connection::open(&db_path).unwrap();
                        let response = format!("{{\"status\":\"ok\",\"bytes_read\":{}}}", n);
                        let _ = stream.write_all(response.as_bytes()).await;
                    }
                }
            });
        }
    });

    Ok(())
}
