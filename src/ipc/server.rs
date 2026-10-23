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
    telemetry: Arc<TelemetryMetrics>,
    consensus_engine: Arc<Mutex<ConsensusEngine>>,
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
                    let tm = Arc::clone(&telemetry);
                    let consensus = Arc::clone(&consensus_engine);

                    tokio::spawn(async move {
                        let mut buf = [0u8; 1024];
                        if let Ok(n) = socket.read(&mut buf) {
                            let command = String::from_utf8_lossy(&buf[..n]).trim().to_string();
                            
                            let response = match command.as_str() {
                                "status" => {
                                    let peers = pm.get_peers();
                                    let engine = consensus.lock().await;
                                    format!(
                                        "DAEMON_STATUS: RUNNING\nRole: {:?}\nCurrent Term: {}\nCommit Index: {}\nConnected Peers: {}\n",
                                        engine.role, engine.current_term, engine.commit_index, peers.len()
                                    )
                                }
                                "peers" => {
                                    let peers = pm.get_peers();
                                    format!("CONNECTED_PEERS:\n{:#?}", peers)
                                }
                                "metrics" => {
                                    let metrics = tm.get_snapshot();
                                    format!("TELEMETRY:\n{:#?}", metrics)
                                }
                                _ => format!("UNKNOWN_COMMAND: {}\n", command),
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
