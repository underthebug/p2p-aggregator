use p2p_aggregator::journal::{rocks_store, sqlite_store};
use p2p_aggregator::ipc::socket;
use p2p_aggregator::network::swarm;
use libp2p::swarm::SwarmEvent;
use libp2p::futures::StreamExt;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing Autonomous Agent Swarm Infrastructure Node...");

    let rocks_path = "./data/telemetry_db";
    std::fs::create_dir_all(rocks_path)?;
    let _telemetry_db = rocks_store::init_telemetry_db(rocks_path)?;
    println!(" [x] RocksDB telemetry store active at {}", rocks_path);

    let sqlite_path = "./data/state.db";
    if let Some(parent) = Path::new(sqlite_path).parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _state_conn = sqlite_store::init_state_db(sqlite_path)?;
    println!(" [x] SQLite WAL transactional database active at {}", sqlite_path);

    let (mut swarm, peer_id) = swarm::build_p2p_node().await?;
    println!(" [x] libp2p node initialized successfully. Local PeerID: {}", peer_id);

    let socket_path = "/tmp/agent_swarm.sock";
    tokio::spawn(async move {
        if let Err(e) = socket::start_socket_listener(socket_path).await {
            eprintln!("IPC socket listener terminated: {}", e);
        }
    });

    println!("Node fully operational. Entering main event loop...");

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(swarm::AgentSwarmBehaviourEvent::Mdns(libp2p::mdns::Event::Discovered(peers))) => {
                    for (peer_id, addr) in peers {
                        println!("[mDNS] Discovered peer: {} at addr {}", peer_id, addr);
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                    }
                }
                SwarmEvent::Behaviour(swarm::AgentSwarmBehaviourEvent::Mdns(libp2p::mdns::Event::Expired(peers))) => {
                    for (peer_id, _addr) in peers {
                        println!("[mDNS] Peer expired: {}", peer_id);
                    }
                }
                SwarmEvent::Behaviour(swarm::AgentSwarmBehaviourEvent::Gossipsub(libp2p::gossipsub::Event::Message { propagation_source: _, message_id: _, message })) => {
                    println!("[Gossipsub] Received message from topic '{}': {} bytes", message.topic, message.data.len());
                }
                _ => {}
            },
            _ = tokio::signal::ctrl_c() => {
                println!("Shutdown signal received. Flushing journals and exiting cleanly.");
                break;
            }
        }
    }

    Ok(())
}
