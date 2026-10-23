use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::path::Path;

pub async fn start_socket_listener(socket_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(socket_path);
    if path.exists() {
        tokio::fs::remove_file(path).await?;
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
            let response = if cmd.trim() == "list" {
                "[0.00] Autonomous Node Infrastructure (Votes: 1) - https://example.com/infrastructure
----------------------------
".to_string()
            } else {
                format!("Processed Command: {}
", cmd.trim())
            };

            if let Err(e) = socket.write_all(response.as_bytes()).await {
                eprintln!("Socket write error: {}", e);
            }
            let _ = socket.shutdown().await;
        });
    }
}
