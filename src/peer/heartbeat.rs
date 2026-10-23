use std::sync::Arc;
use tokio::time::{sleep, Duration};
use crate::peer::manager::PeerManager;
use crate::peer::client::establish_peer_connection;

pub fn start_peer_heartbeat(peer_manager: Arc<PeerManager>, interval_secs: u64) {
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(interval_secs)).await;
            let peers = peer_manager.get_peers();
            if peers.is_empty() {
                continue;
            }
            println!("[Heartbeat] Checking health of {} registered peers...", peers.len());
            for peer in peers {
                match establish_peer_connection(&peer).await {
                    Ok(_) => {
                        // Peer is healthy and responsive
                    }
                    Err(e) => {
                        println!("[Heartbeat] Peer {} failed health check: {}. Pruning from store.", peer, e);
                        peer_manager.remove_peer(&peer);
                    }
                }
            }
        }
    });
}
