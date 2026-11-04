use std::os::unix::net::UnixListener;
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::peer::manager::PeerManager;
use crate::telemetry::worker::TelemetryMetrics;
use crate::consensus::ConsensusEngine;

pub fn start_ipc_server(
    socket_path: &str,
    peer_manager: Arc<PeerManager>,
    _telemetry: Arc<TelemetryMetrics>,
    consensus_engine: Arc<Mutex<ConsensusEngine>>,
    node_id: String,
) -> std::io::Result<()> {
    if std::path::Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }

    let listener = UnixListener::bind(socket_path)?;
    println!("IPC server listening on Unix socket: {}", socket_path);

    tokio::spawn(async move {
        for stream in listener.incoming() {
            match stream {
                Ok(mut socket) => {
                    let pm = Arc::clone(&peer_manager);
                    let consensus = Arc::clone(&consensus_engine);
                    let nid = node_id.clone();

                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        if let Ok(n) = socket.read(&mut buf) {
                            let command = String::from_utf8_lossy(&buf[..n]).trim().to_string();
                            
                            let response = if command == "status" {
                                let peers = pm.get_peers();
                                let engine = consensus.lock().await;
                                format!(
                                    "DAEMON_STATUS: RUNNING\nRole: {:?}\nCurrent Term: {}\nCommit Index: {}\nConnected Peers: {}\n",
                                    engine.role, engine.current_term, engine.commit_index, peers.len()
                                )
                            } else if command == "peers" {
                                let peers = pm.get_peers();
                                format!("CONNECTED_PEERS:\n{:#?}", peers)
                            } else if command == "metrics" {
                                format!("TELEMETRY: Active worker running\n")
                            } else if command.starts_with("submit ") {
                                let payload = command.trim_start_matches("submit ").as_bytes().to_vec();
                                match ConsensusEngine::client_append_entry(consensus, pm, nid, payload).await {
                                    Ok(msg) => msg,
                                    Err(err) => format!("ERROR: {}\n", err),
                                }
                            } else {
                                format!("UNKNOWN_COMMAND: {}\n", command)
                            };

                            let _ = socket.write_all(response.as_bytes());
                        }
                    });
                }
                Err(e) => {
                    eprintln!("IPC accept error: {}", e);
                }
            }
        }
    });

    Ok(())
}
