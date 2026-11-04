use libp2p::{
    gossipsub::{self, IdentTopic, MessageAuthenticity, ValidationMode},
    swarm::SwarmEvent,
    identity, PeerId,
    futures::StreamExt,
};
use rusqlite::Connection;
use std::error::Error;
use tokio::time::Duration;
use std::collections::HashSet;
use p2p_aggregator::identity::{public_key_from_did, verify_broadcast_signature, BroadcastMessage};

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
    p2p_aggregator::migrations::migrate(&conn)?;
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
    let default_regions = vec!["p2p-aggregator-v1", "#global", "#pottstown", "#tech"];
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
                    if let Ok(broadcast) = serde_json::from_slice::<BroadcastMessage>(&message.data) {
                        let topic_str = message.topic.as_str().to_string();
                        let valid = public_key_from_did(&broadcast.sender_did)
                            .is_some_and(|public_key| verify_broadcast_signature(
                                &public_key,
                                &broadcast.id,
                                &broadcast.region,
                                &broadcast.sender_did,
                                &broadcast.sender_type,
                                &broadcast.content,
                                broadcast.magnet_link.as_deref(),
                                broadcast.timestamp,
                                broadcast.ai_tag.as_deref(),
                                &broadcast.signature_hex,
                            ));
                        if !valid {
                            eprintln!("[GossipSub] Rejected invalid signature on topic [{topic_str}]");
                            continue;
                        }
                        println!("⚡ [GossipSub] Verified broadcast {} from {}", broadcast.id, broadcast.sender_did);

                        if let Ok(db_conn) = Connection::open(&db_path) {
                            let inserted = db_conn.execute(
                                "INSERT OR IGNORE INTO secure_broadcasts
                                 (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp, ai_tag)
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                                rusqlite::params![
                                    broadcast.id,
                                    broadcast.region,
                                    broadcast.sender_did,
                                    broadcast.sender_type,
                                    broadcast.content,
                                    broadcast.magnet_link,
                                    broadcast.signature_hex,
                                    broadcast.timestamp,
                                    broadcast.ai_tag,
                                ],
                            );
                            if inserted.is_ok() {
                                seen_broadcasts.insert(broadcast.id);
                            }
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
                    let mut stmt = db_conn.prepare(
                        "SELECT id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp, ai_tag
                         FROM secure_broadcasts ORDER BY timestamp DESC LIMIT 10"
                    ).unwrap();
                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, Option<String>>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, i64>(7)?,
                            row.get::<_, Option<String>>(8)?,
                        ))
                    });

                    if let Ok(iter) = rows {
                        for r in iter.flatten() {
                            let (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp, ai_tag) = r;
                            if !seen_broadcasts.contains(&id) {
                                seen_broadcasts.insert(id.clone());
                                let Some(public_key) = public_key_from_did(&sender_did) else {
                                    eprintln!("[Database Sync] Skipping broadcast with invalid DID: {id}");
                                    continue;
                                };
                                if !verify_broadcast_signature(
                                    &public_key,
                                    &id,
                                    &region,
                                    &sender_did,
                                    &sender_type,
                                    &content,
                                    magnet_link.as_deref(),
                                    timestamp,
                                    ai_tag.as_deref(),
                                    &signature_hex,
                                ) {
                                    eprintln!("[Database Sync] Skipping broadcast with invalid signature: {id}");
                                    continue;
                                }
                                let broadcast = BroadcastMessage {
                                    id,
                                    region,
                                    sender_did,
                                    sender_type,
                                    content,
                                    magnet_link,
                                    signature_hex,
                                    timestamp,
                                    ai_tag,
                                };
                                if let Ok(bytes) = serde_json::to_vec(&broadcast) {
                                    println!("📤 [Database Sync] Publishing signed broadcast {}", broadcast.id);
                                    let topic = IdentTopic::new("p2p-aggregator-v1");
                                    let _ = swarm.behaviour_mut().gossipsub.publish(topic, bytes);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
