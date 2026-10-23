use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub database_path: String,
    pub ipc_socket_path: String,
    pub p2p_bind_addr: String,
    pub heartbeat_interval_secs: u64,
    pub bootstrap_nodes: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        let db_path = if fs::metadata("/var/lib/p2p-aggregator").is_ok() {
            "/var/lib/p2p-aggregator/peers.db".to_string()
        } else {
            "peers.db".to_string()
        };

        Self {
            database_path: db_path,
            ipc_socket_path: "/tmp/agent_swarm.sock".to_string(),
            p2p_bind_addr: "0.0.0.0:4001".to_string(),
            heartbeat_interval_secs: 30,
            bootstrap_nodes: vec![
                "/ip4/127.0.0.1/tcp/4001".to_string(),
            ],
        }
    }
}

impl Config {
    pub fn load<P: AsRef<Path>>(path: P) -> Self {
        if let Ok(contents) = fs::read_to_string(path) {
            if let Ok(config) = toml::from_str(&contents) {
                println!("Loaded configuration from config.toml");
                return config;
            } else {
                eprintln!("Failed to parse config.toml, falling back to defaults.");
            }
        } else {
            // Write a default config file for convenience if it doesn't exist
            let default_cfg = Self::default();
            if let Ok(toml_str) = toml::to_string_pretty(&default_cfg) {
                let _ = fs::write("config.toml", toml_str);
            }
        }
        Self::default()
    }
}
