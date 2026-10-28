use libp2p::{
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    mdns, swarm::NetworkBehaviour, Swarm,
};
use std::error::Error;
use std::time::Duration;

#[derive(NetworkBehaviour)]
pub struct SwarmBehaviourStruct {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

pub async fn build_swarm(keypair: &libp2p::identity::Keypair, port: u16) -> Result<(Swarm<SwarmBehaviourStruct>, IdentTopic), Box<dyn Error>> {
    let topic = IdentTopic::new("p2p-aggregator-main");
    let topic_clone = topic.clone();

    let swarm = libp2p::SwarmBuilder::with_existing_identity(keypair.clone())
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(move |key| {
            let message_authenticity = MessageAuthenticity::Signed(key.clone());
            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                .validation_mode(ValidationMode::Strict)
                .build()
                .expect("Valid gossipsub config");
            
            let mut gossipsub = gossipsub::Behaviour::new(message_authenticity, gossipsub_config)
                .expect("Valid gossipsub instance");
            
            gossipsub.subscribe(&topic_clone).expect("Successfully subscribe to topic");

            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())
                .expect("Valid mdns instance");

            SwarmBehaviourStruct { gossipsub, mdns }
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(30)))
        .build();

    let mut swarm = swarm;
    swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", port).parse()?)?;

    Ok((swarm, topic))
}
