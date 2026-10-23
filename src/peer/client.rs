use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use std::net::SocketAddr;

pub fn parse_multiaddr(addr: &str) -> Option<SocketAddr> {
    // Basic multiaddr parser for /ip4/X.X.X.X/tcp/YYYY or direct IP:PORT
    let trimmed = addr.trim();
    if trimmed.starts_with("/ip4/") {
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts.len() >= 5 {
            let ip = parts[2];
            let port = parts[4];
            if let Ok(socket_addr) = format!("{}:{}", ip, port).parse() {
                return Some(socket_addr);
            }
        }
    } else if let Ok(socket_addr) = trimmed.parse() {
        return Some(socket_addr);
    }
    None
}

pub async fn establish_peer_connection(addr: &str) -> Result<String, String> {
    let socket_addr = parse_multiaddr(addr)
        .ok_or_else(|| format!("Invalid peer address format: {}", addr))?;

    println!("Attempting TCP connection to peer at {}", socket_addr);

    match timeout(Duration::from_secs(3), TcpStream::connect(socket_addr)).await {
        Ok(Ok(_stream)) => {
            Ok(format!("Successfully established TCP handshake with {}", addr))
        }
        Ok(Err(e)) => {
            Err(format!("Failed to connect to {}: {}", addr, e))
        }
        Err(_) => {
            Err(format!("Connection timed out connecting to {}", addr))
        }
    }
}
