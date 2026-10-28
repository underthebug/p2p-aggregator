use libp2p::{
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    mdns, swarm::NetworkBehaviour, PeerId, Swarm,
};
use std::error::Error;
use std::time::Duration;

use crate::swarm_protocol::SwarmMessage;

#[derive(NetworkBehaviour)]
pub struct SwarmBehaviourStruct {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

pub async fn build_swarm(keypair: &libp2p::identity::Keypair) -> Result<(Swarm<SwarmBehaviourStruct>, IdentTopic), Box<dyn Error>> {
    let transport = libp2p::tokio_development_transport(keypair)?;

    let message_authenticity = MessageAuthenticity::Signed(keypair.clone());
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(1))
        .validation_mode(ValidationMode::Strict)
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let mut gossipsub = gossipsub::Behaviour::new(message_authenticity, gossipsub_config)?;

    let topic = IdentTopic::new("p2p-aggregator-main");
    gossipsub.subscribe(&topic)?;

    let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), keypair.public().to_peer_id())?;

    let behavior = SwarmBehaviourStruct { gossipsub, mdns };

    let mut swarm = Swarm::new(transport, behavior, keypair.public().to_peer_id(), libp2p::tokio::Config::default());

    swarm.listen_on("/ip4/0.0.0.0/tcp/41871".parse()?)?;

    Ok((swarm, topic))
}
