mod ipc;
mod telemetry;
mod peer;

use ipc::server::start_ipc_server;
use telemetry::worker::start_telemetry_worker;
use peer::bootstrap::load_bootstrap_nodes;

fn main() -> std::io::Result<()> {
    println!("=== P2P Aggregator Daemon Starting ===");
    
    // Load static bootstrap nodes
    let bootstrap_nodes = load_bootstrap_nodes();
    println!("Loaded {} bootstrap nodes", bootstrap_nodes.len());

    // Start telemetry worker thread
    start_telemetry_worker();

    // Start IPC server over Unix domain socket
    let socket_path = "/tmp/agent_swarm.sock";
    start_ipc_server(socket_path)?;

    // Keep main thread alive
    std::thread::park();
    Ok(())
}
