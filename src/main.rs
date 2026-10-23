mod ipc;
mod telemetry;

use ipc::server::start_ipc_server;
use telemetry::worker::start_telemetry_worker;

fn main() -> std::io::Result<()> {
    println!("=== P2P Aggregator Daemon Starting ===");
    
    // Start telemetry worker thread
    start_telemetry_worker();

    // Start IPC server over Unix domain socket
    let socket_path = "/tmp/agent_swarm.sock";
    start_ipc_server(socket_path)?;

    // Keep main thread alive
    std::thread::park();
    Ok(())
}
