use libp2p::{
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    swarm::SwarmEvent,
    identity, PeerId,
    futures::StreamExt,
};
use rusqlite::Connection;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Duration;
use std::collections::HashSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let port = args.iter().position(|r| r == "--port").and_then(|i| args.get(i + 1)).unwrap_or(&"41871".to_string()).clone();
    let db_path = format!("./data/state_{}.db", port);
    let listen_port: u16 = port.parse().unwrap_or(41871);

    println!("🚀 [p2p-daemon] Starting background sovereign node on port {}...", listen_port);
    println!("📂 [Database]: {}", db_path);
    println!("────────────────────────────────────────────────────────────");

    // Ensure database and tables exist
    let conn = Connection::open(&db_path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS secure_broadcasts (
            id TEXT PRIMARY KEY,
            region TEXT,
            sender_did TEXT,
            sender_type TEXT,
            content TEXT,
            magnet_link TEXT,
            signature_hex TEXT,
            timestamp INTEGER
        )",
        [],
    )?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS regions (
            region TEXT PRIMARY KEY,
            is_subscribed BOOLEAN DEFAULT 0,
            last_seen INTEGER
        )",
        [],
    )?;

    // Generate libp2p identity
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("🔑 [libp2p] Local PeerId: {}", local_peer_id);

    // Set up GossipSub behavior
    let gossipsub_config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(1))
        .validation_mode(ValidationMode::Strict)
        .build()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    let mut gossipsub = gossipsub::Behaviour::new(
        MessageAuthenticity::Signed(local_key.clone()),
        gossipsub_config,
    )?;

    // Subscribe to default core regions
    let default_regions = vec!["#global", "#pottstown", "#tech"];
    for reg in &default_regions {
        let topic = IdentTopic::new(*reg);
        gossipsub.subscribe(&topic)?;
        println!("📻 [GossipSub] Subscribed to region topic: {}", reg);
    }

    #[derive(libp2p::swarm::NetworkBehaviour)]
    struct MyBehaviour {
        gossipsub: gossipsub::Behaviour,
    }

    let behaviour = MyBehaviour { gossipsub };

    // Build Swarm cleanly with TCP + Noise + Yamux
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(libp2p::tcp::Config::default(), libp2p::noise::Config::new, libp2p::yamux::Config::default)?
        .with_behaviour(|_| behaviour)?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    swarm.listen_on(format!("/ip4/0.0.0.0/tcp/{}", listen_port).parse()?)?;

    // Track broadcast IDs already published by this daemon instance to prevent loops
    let mut seen_broadcasts = HashSet::new();
    
    // Seed existing DB records into seen set so we don't spam old history on boot
    if let Ok(db_conn) = Connection::open(&db_path) {
        if let Ok(mut stmt) = db_conn.prepare("SELECT id FROM secure_broadcasts") {
            let rows = stmt.query_map([], |row| row.get::<_, String>(0));
            if let Ok(iter) = rows {
                for id in iter.flatten() {
                    seen_broadcasts.insert(id);
                }
            }
        }
    }

    loop {
        tokio::select! {
            event = swarm.select_next_some() => match event {
                SwarmEvent::Behaviour(MyBehaviourEvent::Gossipsub(gossipsub::Event::Message {
                    propagation_source: _,
                    message_id: _,
                    message,
                })) => {
                    if let Ok(text) = String::from_utf8(message.data) {
                        let topic_str = message.topic.as_str().to_string();
                        println!("⚡ [GossipSub] Received packet on [{}] -> len: {} bytes", topic_str, text.len());
                        
                        // Insert incoming gossip into SQLite
                        if let Ok(db_conn) = Connection::open(&db_path) {
                            let id = uuid::Uuid::new_v4().to_string();
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
                            let _ = db_conn.execute(
                                "INSERT OR IGNORE INTO secure_broadcasts (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp) 
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                                rusqlite::params![
                                    id,
                                    topic_str,
                                    "did:p2p:peer-node",
                                    "human",
                                    text,
                                    Option::<String>::None,
                                    "gossipsub-verified",
                                    now
                                ],
                            );
                            seen_broadcasts.insert(id);
                        }
                    }
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("🌐 [Network] Listening on {}", address);
                }
                _ => {}
            },
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                // Poll SQLite for newly composed local broadcasts to publish over GossipSub
                if let Ok(db_conn) = Connection::open(&db_path) {
                    let mut stmt = db_conn.prepare("SELECT id, region, content FROM secure_broadcasts ORDER BY timestamp DESC LIMIT 10").unwrap();
                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    });

                    if let Ok(iter) = rows {
                        for r in iter.flatten() {
                            let (id, region, content) = r;
                            if !seen_broadcasts.contains(&id) {
                                seen_broadcasts.insert(id.clone());
                                println!("📤 [Database Sync] Found new local broadcast [{}] -> Publishing to mesh...", region);
                                
                                let topic = IdentTopic::new(&region);
                                let _ = swarm.behaviour_mut().gossipsub.publish(topic, content.into_bytes());
                            }
                        }
                    }
                }
            }
        }
    }
}
