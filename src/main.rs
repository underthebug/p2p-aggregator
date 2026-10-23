mod ipc;
mod telemetry;
mod peer;

use ipc::server::start_ipc_server;
use telemetry::worker::start_telemetry_worker;
use peer::bootstrap::load_bootstrap_nodes;
use peer::manager::PeerManager;
use std::sync::Arc;

fn main() -> std::io::Result<()> {
    println!("=== P2P Aggregator Daemon Starting ===");
    
    // Initialize Shared Peer Manager & load bootstrap nodes
    let peer_manager = Arc::new(PeerManager::new());
    let bootstrap_nodes = load_bootstrap_nodes();
    
    for node in bootstrap_nodes {
        peer_manager.add_peer(node);
    }
    println!("Initialized PeerManager with {} active peer entries", peer_manager.get_peer_count());

    // Start telemetry worker thread
    start_telemetry_worker();

    // Start IPC server over Unix domain socket with shared peer manager state
    let socket_path = "/tmp/agent_swarm.sock";
    start_ipc_server(socket_path, Arc::clone(&peer_manager))?;

    // Keep main thread alive
    std::thread::park();
    Ok(())
}
