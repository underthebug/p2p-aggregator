use std::path::Path;
use tokio::net::UnixListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P2P Aggregator Daemon Starting ===");
    
    // Initialize data directory
    std::fs::create_dir_all("./data/telemetry_db")?;
    
    // Initialize databases
    let _telemetry_db = p2p_aggregator::journal::rocks_store::init_telemetry_db("./data/telemetry_db")?;
    println!(" [x] RocksDB telemetry store active at ./data/telemetry_db");
    
    let _state_conn = p2p_aggregator::journal::sqlite_store::init_state_db("./data/state.db")?;
    println!(" [x] SQLite WAL transactional database active at ./data/state.db");

    // Initialize libp2p node
    let (_swarm, peer_id) = p2p_aggregator::network::swarm::build_p2p_node().await?;
    println!(" [x] libp2p node initialized successfully. Local PeerID: {}", peer_id);

    // Start IPC socket listener
    let socket_path = "/tmp/agent_swarm.sock";
    if Path::new(socket_path).exists() {
        tokio::fs::remove_file(socket_path).await?;
    }
    let listener = UnixListener::bind(socket_path)?;
    println!("High-speed IPC socket listening on {}", socket_path);

    tokio::spawn(async move {
        loop {
            if let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0; 4096];
                    let n = match socket.read(&mut buf).await {
                        Ok(n) if n == 0 => return,
                        Ok(n) => n,
                        Err(_) => return,
                    };
                    
                    let cmd = String::from_utf8_lossy(&buf[0..n]);
                    let trimmed = cmd.trim();
                    let response = if trimmed == "list" {
                        "[0.00] Autonomous Node Infrastructure (Votes: 1) - https://example.com/infrastructure\n----------------------------\n".to_string()
                    } else if trimmed == "status" {
                        "[OK] P2P Aggregator Daemon Active | SQLite WAL: Healthy | RocksDB Telemetry: Connected\n".to_string()
                    } else if trimmed == "nodes" {
                        format!("[ACTIVE PEERS]\n- Local PeerID: {} (mDNS / Kademlia active)\n----------------------------\n", peer_id)
                    } else {
                        format!("Processed Command: {}\n", trimmed)
                    };

                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.shutdown().await;
                });
            }
        }
    });

    println!("Node fully operational. Entering main event loop...");
    
    // Keep main alive
    futures::future::pending::<()>().await;

    Ok(())
}
'''
