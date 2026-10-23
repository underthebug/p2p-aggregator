use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use crate::peer::manager::PeerManager;
use crate::crypto::NodeIdentity;
use rand::rngs::OsRng;
use rand::RngCore;

pub async fn start_p2p_listener(bind_addr: &str, peer_manager: Arc<PeerManager>) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    println!("P2P TCP encrypted listener active on {}", bind_addr);

    tokio::spawn(async move {
        let identity = Arc::new(NodeIdentity::load_or_generate("node.key"));

        loop {
            match listener.accept().await {
                Ok((mut socket, peer_addr)) => {
                    let pm = Arc::clone(&peer_manager);
                    let id = Arc::clone(&identity);
                    
                    tokio::spawn(async move {
                        // 1. Read remote peer's public key (32 bytes)
                        let mut remote_pubkey = [0u8; 32];
                        if socket.read_exact(&mut remote_pubkey).await.is_err() {
                            return;
                        }

                        // 2. Send our public key (32 bytes)
                        let pub_key_bytes = id.verifying_key.to_bytes();
                        if socket.write_all(&pub_key_bytes).await.is_err() {
                            return;
                        }

                        // 3. Generate and send random challenge nonce (32 bytes)
                        let mut challenge = [0u8; 32];
                        OsRng.fill_bytes(&mut challenge);
                        if socket.write_all(&challenge).await.is_err() {
                            return;
                        }

                        // 4. Read remote peer's signature (64 bytes)
                        let mut sig_bytes = [0u8; 64];
                        if socket.read_exact(&mut sig_bytes).await.is_err() {
                            return;
                        }

                        // 5. Verify signature
                        let authenticated = id.verify_challenge(&remote_pubkey, &challenge, &sig_bytes);
                        
                        if authenticated {
                            let _ = socket.write_all(b"OK").await;
                            pm.add_peer(peer_addr.to_string());
                            println!("[CryptoMesh] Successfully authenticated secure peer: {}", peer_addr);
                        } else {
                            let _ = socket.write_all(b"ER").await;
                            println!("[CryptoMesh] Authentication FAILED for peer: {}", peer_addr);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("P2P accept error: {}", e);
                }
            }
        }
    });

    Ok(())
}
