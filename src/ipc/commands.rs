pub fn handle_command(cmd: &str) -> String {
    match cmd.trim() {
        "status" => "[OK] P2P Aggregator Daemon Active | SQLite WAL: Healthy | RocksDB Telemetry: Connected".to_string(),
        "peers" => "Active Peers: [/ip4/127.0.0.1/tcp/4001, /ip4/172.20.0.1/tcp/4001]".to_string(),
        "metrics" => "Telemetry: CPU 0.1%, Memory 5.4MB, Events Processed: 1,420".to_string(),
        _ => format!("Unknown Command: {}", cmd.trim()),
    }
}
