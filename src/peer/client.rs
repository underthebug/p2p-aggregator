use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::error::Error;
use crate::crypto::NodeIdentity;

pub async fn establish_peer_connection(addr: &str) -> Result<String, Box<dyn Error + Send + Sync>> {
    let mut socket = TcpStream::connect(addr).await?;
    
    // Load local node identity for cryptographic authentication
    let identity = NodeIdentity::load_or_generate("node.key");
    let pub_key_bytes = identity.verifying_key.to_bytes();

    // 1. Send our public key (32 bytes)
    socket.write_all(&pub_key_bytes).await?;

    // 2. Read remote peer's public key (32 bytes)
    let mut remote_pubkey = [0u8; 32];
    socket.read_exact(&mut remote_pubkey).await?;

    // 3. Receive challenge nonce from remote peer (32 bytes)
    let mut challenge = [0u8; 32];
    socket.read_exact(&mut challenge).await?;

    // 4. Sign challenge and send signature (64 bytes)
    let signature = identity.sign_challenge(&challenge);
    socket.write_all(&signature.to_bytes()).await?;

    // 5. Read handshake status result
    let mut status_buf = [0u8; 2];
    socket.read_exact(&mut status_buf).await?;
    
    if &status_buf == b"OK" {
        Ok(format!("Successfully authenticated and connected to peer {}", addr))
    } else {
        Err("Cryptographic handshake authentication failed".into())
    }
}
