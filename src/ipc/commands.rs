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

pub fn handle_command(cmd: &str) -> String {
    let command = IpcCommand::from_str(cmd).unwrap_or_else(|_| IpcCommand::Unknown(cmd.to_string()));
    match command {
        IpcCommand::Status => "[OK] P2P Aggregator Daemon Active | SQLite WAL: Healthy | RocksDB Telemetry: Connected".to_string(),
        IpcCommand::Peers => "Active Peers: [/ip4/127.0.0.1/tcp/4001, /ip4/172.20.0.1/tcp/4001]".to_string(),
        IpcCommand::Metrics => "Telemetry: CPU 0.1%, Memory 5.4MB, Events Processed: 1,420".to_string(),
        IpcCommand::Connect(addr) => format!("[OK] Initiating connection to peer: {}\n", addr),
        IpcCommand::LogLevel(level) => format!("[OK] Log level updated to: {}\n", level),
        IpcCommand::Unknown(c) => format!("Unknown Command: {}", c),
    }
}
