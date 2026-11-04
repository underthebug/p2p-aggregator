use libp2p::{
    gossipsub, kad, mdns, swarm::NetworkBehaviour, PeerId
};

#[derive(NetworkBehaviour)]
pub struct AggregatorBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub mdns: mdns::tokio::Behaviour,
}

pub fn create_behaviour(peer_id: PeerId) -> Result<AggregatorBehaviour, Box<dyn std::error::Error>> {
    let message_authenticator = gossipsub::MessageAuthenticity::Signed(libp2p::identity::Keypair::generate_ed25519());
    let mut gossipsub_config = gossipsub::ConfigBuilder::default();
    gossipsub_config.mesh_n_low(2);
    let gossipsub_config = gossipsub_config.build().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    
    let gossipsub = gossipsub::Behaviour::new(message_authenticator, gossipsub_config)?;

    let store = kad::store::MemoryStore::new(peer_id);
    let kademlia = kad::Behaviour::new(peer_id, store);

    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), peer_id)?;

    Ok(AggregatorBehaviour {
        gossipsub,
        kademlia,
        mdns,
    })
}
