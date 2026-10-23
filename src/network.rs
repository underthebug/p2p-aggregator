pub mod swarm {
    use libp2p::swarm::NetworkBehaviour;
    #[derive(NetworkBehaviour)]
    pub struct AggregatorBehaviour {
        pub mdns: libp2p::mdns::tokio::Behaviour,
        pub gossipsub: libp2p::gossipsub::Behaviour,
    }

    pub fn create_behaviour(_keypair: &libp2p::identity::Keypair) -> Result<AggregatorBehaviour, Box<dyn std::error::Error>> {
        let mdns = libp2p::mdns::tokio::Behaviour::new(libp2p::mdns::Config::default(), _keypair.public().to_peer_id())?;
        let message_id_fn = |message: &libp2p::gossipsub::Message| {
            let mut s = std::collections::hash_map::DefaultHasher::new();
            std::hash::Hash::hash(&message.data, &mut s);
            libp2p::gossipsub::MessageId::from(std::hash::Hasher::finish(&s).to_string())
        };
        let gossipsub_config = libp2p::gossipsub::ConfigBuilder::default()
            .message_id_fn(message_id_fn)
            .build()?;
        let gossipsub = libp2p::gossipsub::Behaviour::new(
            libp2p::gossipsub::MessageAuthenticity::Signed(_keypair.clone()),
            gossipsub_config,
        )?;
        Ok(AggregatorBehaviour { mdns, gossipsub })
    }
}
pub use swarm::*;
