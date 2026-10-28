use libp2p::{
    gossipsub, mdns, swarm::{NetworkBehaviour, SwarmEvent}, identity, PeerId,
};
use futures::StreamExt;
use std::error::Error;
use std::sync::Arc;
use crate::db::{DbManager, SecureBroadcast};

#[derive(NetworkBehaviour)]
pub struct SovereignBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

pub async fn start_p2p_listener(db_manager: Arc<DbManager>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = PeerId::from(id_keys.public());
    println!("Local Sovereign peer id: {peer_id}");

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .mesh_n_low(2)
                .mesh_n_high(6)
                .build()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;

            Ok(SovereignBehaviour { gossipsub, mdns })
        })?
        .build();

    let topic = gossipsub::IdentTopic::new("sovereign-mesh-v1");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    match swarm.listen_on("/ip4/0.0.0.0/tcp/41871".parse()?) {
        Ok(addr) => println!("libp2p successfully bound and listening on {addr}"),
        Err(err) => eprintln!("CRITICAL: Failed to bind libp2p listener on port 41871: {:?}", err),
    }

    tokio::spawn(async move {
        while let Some(event) = swarm.next().await {
            match event {
                SwarmEvent::Behaviour(SovereignBehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                    for (peer_id, _addr) in list {
                        println!("mDNS discovered mesh peer: {peer_id}");
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::Behaviour(SovereignBehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
                    for (peer_id, _addr) in list {
                        println!("mDNS peer expired: {peer_id}");
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    }
                }
                SwarmEvent::Behaviour(SovereignBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    message,
                    ..
                })) => {
                    if let Ok(broadcast) = serde_json::from_slice::<SecureBroadcast>(&message.data) {
                        println!("Mesh Broadcast received from {}: {}", broadcast.sender_did, broadcast.content);
                        if let Err(e) = db_manager.upsert_broadcast(&broadcast) {
                            eprintln!("Failed to store incoming mesh broadcast in SQLite: {e}");
                        }
                    }
                }
                _ => {}
            }
        }
    });

    Ok(())
}
