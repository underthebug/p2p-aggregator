use serde::Deserialize;
use std::fs;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub p2p_bind_addr: String,
    pub metrics_bind_addr: String,
    pub ipc_socket_path: String,
    pub database_path: String,
    pub heartbeat_interval_secs: u64,
    pub bootstrap_nodes: Vec<String>,
}

impl Config {
    pub fn load(path: &str) -> Self {
        let contents = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => {
                return Self {
                    p2p_bind_addr: "0.0.0.0:40827".to_string(),
                    metrics_bind_addr: "0.0.0.0:9090".to_string(),
                    ipc_socket_path: "/run/p2p-aggregator/p2p-aggregator.sock".to_string(),
                    database_path: "./data/aggregator.db".to_string(),
                    heartbeat_interval_secs: 2,
                    bootstrap_nodes: vec![],
                };
            }
        };

        toml::from_str(&contents).unwrap_or_else(|e| {
            eprintln!("Failed to parse config from {}: {}. Using defaults.", path, e);
            Self {
                p2p_bind_addr: "0.0.0.0:40827".to_string(),
                metrics_bind_addr: "0.0.0.0:9090".to_string(),
                ipc_socket_path: "/run/p2p-aggregator/p2p-aggregator.sock".to_string(),
                database_path: "./data/aggregator.db".to_string(),
                heartbeat_interval_secs: 2,
                bootstrap_nodes: vec![],
            }
        })
    }
}
