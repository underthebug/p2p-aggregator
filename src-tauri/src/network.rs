use libp2p::{
    gossipsub, mdns, swarm::{NetworkBehaviour, SwarmEvent}, identity, PeerId,
};
use futures::StreamExt;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use crate::db::{DbManager, SecureBroadcast};

#[derive(NetworkBehaviour)]
pub struct SovereignBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

fn load_or_create_identity(path: &Path) -> Result<identity::Keypair, Box<dyn Error + Send + Sync>> {
    match fs::read(path) {
        Ok(bytes) => Ok(identity::Keypair::from_protobuf_encoding(&bytes)?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
                fs::create_dir_all(parent)?;
            }
            let key = identity::Keypair::generate_ed25519();
            let encoded = key.to_protobuf_encoding()?;
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(path) {
                Ok(mut file) => {
                    file.write_all(&encoded)?;
                    Ok(key)
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let bytes = fs::read(path)?;
                    Ok(identity::Keypair::from_protobuf_encoding(&bytes)?)
                }
                Err(error) => Err(Box::new(error)),
            }
        }
        Err(error) => Err(Box::new(error)),
    }
}

pub async fn start_p2p_listener(db_manager: Arc<DbManager>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let id_keys = load_or_create_identity(Path::new("data/libp2p.key"))?;
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

    let topic = gossipsub::IdentTopic::new("p2p-aggregator-v1");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    match swarm.listen_on("/ip4/0.0.0.0/tcp/41874".parse()?) {
        Ok(addr) => println!("libp2p successfully bound and listening on {addr}"),
        Err(err) => eprintln!("CRITICAL: Failed to bind libp2p listener on port 41874: {:?}", err),
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

#[cfg(test)]
mod tests {
    use super::load_or_create_identity;
    use libp2p::PeerId;

    #[test]
    fn libp2p_identity_is_stable_across_loads() {
        let path = std::env::temp_dir().join(format!("p2p-libp2p-{}.key", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let first = load_or_create_identity(&path).unwrap();
        let second = load_or_create_identity(&path).unwrap();
        assert_eq!(PeerId::from(first.public()), PeerId::from(second.public()));

        let _ = std::fs::remove_file(path);
    }
}
