
use serde::Deserialize;
use std::fs;

#[derive(Deserialize, Debug, Clone)]
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
        let content = fs::read_to_string(path).unwrap_or_default();
        toml::from_str(&content).unwrap_or_else(|_| Self {
            p2p_bind_addr: "127.0.0.1:9001".to_string(),
            metrics_bind_addr: "127.0.0.1:8001".to_string(),
            ipc_socket_path: "./data/p2p-node1.sock".to_string(),
            database_path: "./data/node1.db".to_string(),
            heartbeat_interval_secs: 1,
            bootstrap_nodes: vec![],
        })
    }
}
