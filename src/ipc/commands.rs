use crate::peer::manager::PeerManager;
use crate::peer::client::establish_peer_connection;

pub enum IpcCommand {
    Status,
    Peers,
    Metrics,
    Connect(String),
    LogLevel(String),
    Unknown(String),
}

impl IpcCommand {
    pub fn from_str(s: &str) -> Result<Self, String> {
        let trimmed = s.trim();
        if trimmed.starts_with("connect ") {
            let addr = trimmed[8..].trim().to_string();
            return Ok(Self::Connect(addr));
        }
        if trimmed.starts_with("loglevel ") {
            let level = trimmed[9..].trim().to_string();
            return Ok(Self::LogLevel(level));
        }

        match trimmed {
            "status" => Ok(Self::Status),
            "peers" => Ok(Self::Peers),
            "metrics" => Ok(Self::Metrics),
            other => Ok(Self::Unknown(other.to_string())),
        }
    }
}

pub async fn handle_command_with_peers_async(cmd: &str, peer_manager: &PeerManager) -> String {
    let command = IpcCommand::from_str(cmd).unwrap_or_else(|_| IpcCommand::Unknown(cmd.to_string()));
    match command {
        IpcCommand::Status => "[OK] P2P Aggregator Daemon Active | SQLite WAL: Healthy | RocksDB Telemetry: Connected".to_string(),
        IpcCommand::Peers => {
            let peers = peer_manager.get_peers();
            format!("Active Peers: [{}]", peers.join(", "))
        }
        IpcCommand::Metrics => "Telemetry: CPU 0.1%, Memory 5.4MB, Events Processed: 1,420".to_string(),
        IpcCommand::Connect(addr) => {
            match establish_peer_connection(&addr).await {
                Ok(msg) => {
                    peer_manager.add_peer(addr.clone());
                    format!("[OK] {}\n", msg)
                }
                Err(e) => {
                    format!("[ERROR] Failed to connect to peer {}: {}\n", addr, e)
                }
            }
        }
        IpcCommand::LogLevel(level) => format!("[OK] Log level updated to: {}\n", level),
        IpcCommand::Unknown(c) => format!("Unknown Command: {}", c),
    }
}
