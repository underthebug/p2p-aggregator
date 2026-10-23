mod config;
mod ipc;
mod telemetry;
mod peer;

use config::Config;
use ipc::server::start_ipc_server;
use telemetry::worker::{start_telemetry_worker, TelemetryMetrics};
use peer::manager::PeerManager;
use peer::server::start_p2p_listener;
use peer::heartbeat::start_peer_heartbeat;
use std::sync::Arc;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    println!("=== P2P Aggregator Daemon Starting ===");
    
    // Load configuration from config.toml (or fallback to defaults)
    let config = Config::load("config.toml");
    
    // Initialize Shared Peer Manager with SQLite WAL persistence
    let peer_manager = Arc::new(PeerManager::with_store(&config.database_path));
    
    for node in &config.bootstrap_nodes {
        peer_manager.add_peer(node.clone());
    }
    println!("Initialized PeerManager with {} active peer entries (Persistence DB: {})", peer_manager.get_peer_count(), config.database_path);

    // Initialize & start live telemetry worker
    let telemetry = TelemetryMetrics::new();
    start_telemetry_worker(Arc::clone(&telemetry));
    println!("Background telemetry worker started.");

    // Start background peer heartbeat and health check worker
    start_peer_heartbeat(Arc::clone(&peer_manager), config.heartbeat_interval_secs);
    println!("Background peer heartbeat worker started (interval: {}s).", config.heartbeat_interval_secs);

    // Start inbound P2P TCP listener
    start_p2p_listener(&config.p2p_bind_addr, Arc::clone(&peer_manager)).await?;

    // Start IPC server over Unix domain socket
    start_ipc_server(&config.ipc_socket_path, Arc::clone(&peer_manager), Arc::clone(&telemetry))?;

    // Keep main task alive
    std::future::pending::<()>().await;
    Ok(())
}
