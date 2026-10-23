use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use crate::peer::manager::PeerManager;

pub async fn start_p2p_listener(bind_addr: &str, peer_manager: Arc<PeerManager>) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    println!("P2P network listener active on {}", bind_addr);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, remote_addr)) => {
                    let pm = Arc::clone(&peer_manager);
                    tokio::spawn(async move {
                        println!("Accepted inbound P2P connection from {}", remote_addr);
                        
                        let mut buf = [0; 256];
                        if let Ok(n) = socket.read(&mut buf).await {
                            if n > 0 {
                                let msg = String::from_utf8_lossy(&buf[..n]);
                                println!("Received peer message from {}: {}", remote_addr, msg.trim());
                            }
                        }

                        // Send protocol acknowledgment
                        let _ = socket.write_all(b"P2P-AGGREGATOR-ACK\n").await;
                        
                        // Automatically register the connected peer address
                        let peer_multiaddr = format!("/ip4/{}/tcp/{}", remote_addr.ip(), remote_addr.port());
                        pm.add_peer(peer_multiaddr);
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
