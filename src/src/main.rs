mod cli;
mod config;
mod crypto;
mod consensus;
mod ipc;
mod telemetry;
mod peer;
mod journal;

use clap::Parser;
use cli::Cli;
use config::Config;
use consensus::ConsensusEngine;
use ipc::server::start_ipc_server;
use telemetry::worker::{start_telemetry_worker, TelemetryMetrics};
use telemetry::exporter::start_metrics_exporter;
use peer::manager::PeerManager;
use peer::server::start_p2p_listener;
use peer::heartbeat::start_peer_heartbeat;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P2P Aggregator Daemon Starting ===");
    
    let args = Cli::parse();
    let mut config = Config::load(&args.config);

    if let Some(port) = args.metrics_port {
        config.metrics_bind_addr = format!("0.0.0.0:{}", port);
    }
    if let Some(port) = args.p2p_port {
        config.p2p_bind_addr = format!("0.0.0.0:{}", port);
    }
    
    let rocks_path = format!("{}_rocks", config.database_path);
    let _telemetry_db = journal::rocks::init_telemetry_db(&rocks_path)?;

    let peer_manager = Arc::new(PeerManager::with_store(&config.database_path));
    let consensus = ConsensusEngine::new(&config.database_path);
    println!("Consensus engine initialized (Term: {}, Role: Follower)", consensus.lock().await.current_term);
    
    for node in &config.bootstrap_nodes {
        peer_manager.add_peer(node.clone());
    }

    let identity = Arc::new(crypto::NodeIdentity::load_or_generate("node.key"));
    let node_id = hex::encode(identity.verifying_key.to_bytes());

    ConsensusEngine::start_leader_heartbeat_loop(
        Arc::clone(&consensus),
        Arc::clone(&peer_manager),
        node_id.clone(),
        config.heartbeat_interval_secs,
    );

    ConsensusEngine::start_election_timer_loop(
        Arc::clone(&consensus),
        Arc::clone(&peer_manager),
        node_id.clone(),
    );

    let telemetry = TelemetryMetrics::new(&config.database_path);
    start_telemetry_worker(Arc::clone(&telemetry));

    start_metrics_exporter(&config.metrics_bind_addr, Arc::clone(&peer_manager), Arc::clone(&telemetry)).await?;
    start_peer_heartbeat(Arc::clone(&peer_manager), config.heartbeat_interval_secs);
    start_p2p_listener(&config.p2p_bind_addr, Arc::clone(&peer_manager), Arc::clone(&consensus)).await?;
    start_ipc_server(&config.ipc_socket_path, Arc::clone(&peer_manager), Arc::clone(&telemetry), Arc::clone(&consensus), node_id)?;

    std::future::pending::<()>().await;
    Ok(())
}
