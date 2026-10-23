mod ipc;
mod telemetry;
mod peer;

use ipc::server::start_ipc_server;
use telemetry::worker::{start_telemetry_worker, TelemetryMetrics};
use peer::bootstrap::load_bootstrap_nodes;
use peer::manager::PeerManager;
use peer::server::start_p2p_listener;
use peer::heartbeat::start_peer_heartbeat;
use std::sync::Arc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    println!("=== P2P Aggregator Daemon Starting ===");
    
    // Initialize Shared Peer Manager with SQLite WAL persistence
    let db_path = "/var/lib/p2p-aggregator/peers.db";
    let db_actual_path = if std::fs::metadata("/var/lib/p2p-aggregator").is_ok() {
        db_path
    } else {
        "peers.db"
    };

    let peer_manager = Arc::new(PeerManager::with_store(db_actual_path));
    let bootstrap_nodes = load_bootstrap_nodes();
    
    for node in bootstrap_nodes {
        peer_manager.add_peer(node);
    }
    println!("Initialized PeerManager with {} active peer entries (Persistence DB: {})", peer_manager.get_peer_count(), db_actual_path);

    // Initialize & start live telemetry worker
    let telemetry = TelemetryMetrics::new();
    start_telemetry_worker(Arc::clone(&telemetry));
    println!("Background telemetry worker started.");

    // Start background peer heartbeat and health check worker (every 30 seconds)
    start_peer_heartbeat(Arc::clone(&peer_manager), 30);
    println!("Background peer heartbeat worker started.");

    // Start inbound P2P TCP listener on port 4001
    let p2p_bind_addr = "0.0.0.0:4001";
    start_p2p_listener(p2p_bind_addr, Arc::clone(&peer_manager)).await?;

    // Start IPC server over Unix domain socket with shared peer manager & telemetry state
    let socket_path = "/tmp/agent_swarm.sock";
    start_ipc_server(socket_path, Arc::clone(&peer_manager), Arc::clone(&telemetry))?;

    // Keep main task alive
    std::future::pending::<()>().await;
    Ok(())
}
