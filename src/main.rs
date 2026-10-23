use p2p_aggregator::journal::{rocks_store, sqlite_store};
use p2p_aggregator::ipc::socket;
use p2p_aggregator::network::swarm::create_behaviour;
use p2p_aggregator::metrics::start_metrics_server;
use p2p_aggregator::wot::verifier::SignedPayload;
use libp2p::swarm::SwarmEvent;
use libp2p::gossipsub::IdentTopic;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== P2P Aggregator Daemon Starting ===");
    std::fs::create_dir_all("./data/telemetry_db")?;

    let _telemetry_db = rocks_store::init_telemetry_db("./data/telemetry_db")?;
    let _state_conn = sqlite_store::init_state_db("./data/state.db")?;

    let (_metrics_addr, _metrics_handle) = start_metrics_server()?;

    let id_keys = libp2p::identity::Keypair::generate_ed25519();
    let _peer_id = id_keys.public().to_peer_id();

    let behaviour = create_behaviour(&id_keys)?;

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|_| behaviour)?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(std::time::Duration::from_secs(30)))
        .build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let telemetry_topic = IdentTopic::new("p2p-aggregator-telemetry");
    swarm.behaviour_mut().gossipsub.subscribe(&telemetry_topic)?;

    let _socket_listener = socket::start_socket_listener("/tmp/agent_swarm.sock", "./data/state.db").await;

    println!("Node fully operational. Entering main event loop...");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Shutdown signal received.");
        }
        _ = async {
            loop {
                match swarm.select_next_some().await {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("Listening on {address}");
                    }
                    SwarmEvent::Behaviour(p2p_aggregator::network::swarm::AggregatorBehaviourEvent::Gossipsub(
                        libp2p::gossipsub::Event::Message { message, .. }
                    )) => {
                        if let Ok(data_str) = String::from_utf8(message.data) {
                            if let Ok(payload) = serde_json::from_str::<SignedPayload>(&data_str) {
                                match payload.verify_signature() {
                                    Ok(true) => println!("Verified telemetry payload received: {}", payload.payload),
                                    _ => println!("Signature verification failed for peer message."),
                                }
                            }
                        }
                    }
                    SwarmEvent::Behaviour(event) => {
                        println!("Swarm behaviour event received: {event:?}");
                    }
                    _ => {}
                }
            }
        } => {}
    }

    Ok(())
}

// TODO: Implement telemetry flush worker and static bootstrap node configuration
