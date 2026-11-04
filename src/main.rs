use libp2p::{futures::StreamExt, swarm::SwarmEvent};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use std::env;
use librqbit::Session;

// Each node needs its own BitTorrent DHT UDP port; sharing the default makes later nodes fail.
const DHT_PORT_BASE: u16 = 60_000;
const DHT_PORT_RANGE: u16 = 5_000;

pub mod swarm_protocol;
pub mod network;
pub mod db;

use swarm_protocol::SwarmMessage;
use network::{build_swarm, SwarmBehaviourStructEvent};
use db::StateDatabase;

#[derive(Debug, PartialEq, Eq)]
enum PublishOutcome {
    StoredLocally,
    Dropped,
}

fn classify_publish_error(err: &libp2p::gossipsub::PublishError) -> PublishOutcome {
    match err {
        libp2p::gossipsub::PublishError::InsufficientPeers => PublishOutcome::StoredLocally,
        _ => PublishOutcome::Dropped,
    }
}

fn dht_listen_port(node_port: u16) -> u16 {
    DHT_PORT_BASE + node_port % DHT_PORT_RANGE
}

async fn persist_local_fallback(db: &Arc<Mutex<StateDatabase>>, msg: &SwarmMessage, port: u16) {
    match msg {
        SwarmMessage::FeedCard { id, author_peer_id, title, url, summary, magnet_link, timestamp } => {
            let mut db = db.lock().await;
            if let Err(e) = db.save_feed_card(
                id,
                author_peer_id,
                title,
                url,
                summary,
                magnet_link.as_deref(),
                *timestamp,
            ) {
                eprintln!("❌ [Port {}] Failed to persist local-only feed card fallback: {}", port, e);
            } else {
                println!("ℹ️ [Gossipsub - Port {}] No mesh peers yet; saved feed card locally for future sync.", port);
            }
        }
        _ => {}
    }
}

#[test]
fn insufficient_peer_publish_is_handled_as_local_only() {
    let outcome = classify_publish_error(&libp2p::gossipsub::PublishError::InsufficientPeers);
    assert_eq!(outcome, PublishOutcome::StoredLocally);
}

#[test]
fn local_nodes_use_distinct_dht_ports() {
    let ports = [41871, 41872, 41873].map(dht_listen_port);
    assert_eq!(ports, [60_000 + 1871, 60_000 + 1872, 60_000 + 1873]);
    assert!(ports.iter().all(|port| *port != 6771));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let port: u16 = if args.len() > 1 {
        args[1].parse().unwrap_or(41871)
    } else {
        41871
    };

    let id_keys = libp2p::identity::Keypair::generate_ed25519();
    let local_peer_id = id_keys.public().to_peer_id();
    println!("🚀 Starting P2P Aggregator Node on Port {} | Peer ID: {}", port, local_peer_id);

    // Initialize SQLite DB
    let db_path = format!("./data/state_{}.db", port);
    let db = StateDatabase::new(&db_path)?;
    let db = Arc::new(Mutex::new(db));
    println!("💾 SQLite transactional state database initialized at {}", db_path);

    // Initialize embedded BitTorrent Session via librqbit
    let download_dir = format!("./data/downloads_{}", port);
    std::fs::create_dir_all(&download_dir)?;
    let mut session_options = librqbit::SessionOptions::default();
    let dht = session_options.dht.get_or_insert_with(Default::default);
    dht.port = Some(dht_listen_port(port));
    dht.persistence = None;
    let bittorrent_session = Session::new_with_opts(download_dir.into(), session_options).await?;
    let _bittorrent_session = Arc::new(bittorrent_session);
    println!("🧲 Embedded BitTorrent engine initialized. Downloads folder: ./data/downloads_{}", port);

    let (mut swarm, topic) = build_swarm(&id_keys, port).await?;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<SwarmMessage>(32);

    println!("✨ Swarm built successfully. Listening for cluster peers and broadcasting feeds...");

    // Extra CLI args are bootstrap multiaddrs (node2/node3 dial node1); P2P_BOOTSTRAP is a comma-separated alternative.
    let env_bootstrap = env::var("P2P_BOOTSTRAP").unwrap_or_default();
    let bootstrap_addrs = args.iter().skip(2).filter(|a| !a.starts_with("--")).map(String::as_str).chain(env_bootstrap.split(',')).map(str::trim).filter(|s| !s.is_empty());
    for raw in bootstrap_addrs {
        match raw.parse::<libp2p::Multiaddr>() {
            Ok(addr) => match swarm.dial(addr.clone()) {
                Ok(()) => println!("🔗 [Port {}] Dialing bootstrap node {}", port, addr),
                Err(e) => eprintln!("⚠️ [Port {}] Failed to dial bootstrap {}: {:?}", port, addr, e),
            },
            Err(e) => eprintln!("⚠️ [Port {}] Invalid bootstrap multiaddr {:?}: {}", port, raw, e),
        }
    }

    let tx_ticker = tx.clone();
    let peer_id_str = local_peer_id.to_string();
    let peer_id_ticker = peer_id_str.clone();
    let initial_delay = if port % 2 == 0 { 3 } else { 6 };
    // Test cards are off unless the operator passes --test-cards or sets P2P_TEST_CARDS=1.
    let test_cards = args.iter().any(|a| a == "--test-cards") || env::var("P2P_TEST_CARDS").map(|v| v == "1").unwrap_or(false);

    // 1. Background task: Publish test feed cards every 10 seconds (opt-in)
    tokio::spawn(async move {
        if !test_cards {
            return;
        }
        tokio::time::sleep(Duration::from_secs(initial_delay)).await;

        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            
            let card_id = uuid::Uuid::new_v4().to_string();
            let card = SwarmMessage::FeedCard {
                id: card_id.clone(),
                author_peer_id: peer_id_ticker.clone(),
                title: format!("Server Node {} Discovery - #{}", port, &card_id[..6]),
                url: "https://news.ycombinator.com".into(),
                summary: format!("AI Swarm Analysis from Port {}: High-bandwidth stream verified.", port),
                magnet_link: None,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };

            if tx_ticker.send(card).await.is_err() {
                break;
            }
        }
    });

    // 2. Background task: Poll SQLite outbox table for CLI-triggered actions
    let db_poller = Arc::clone(&db);
    let tx_poller = tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let mut db = db_poller.lock().await;
            
            if let Ok(pending) = db.get_pending_outbox() {
                let mut processed_ids = Vec::new();
                for (id, json_str) in pending {
                    if db.outbox_already_sent(&json_str).unwrap_or(false) {
                        processed_ids.push(id);
                        continue;
                    }
                    if let Ok(msg) = serde_json::from_str::<SwarmMessage>(&json_str) {
                        if tx_poller.send(msg).await.is_ok() {
                            let _ = db.record_outbox_sent(&json_str);
                            processed_ids.push(id);
                        }
                    }
                }

                for id in processed_ids {
                    let _ = db.mark_outbox_sent(id);
                }
            }
        }
    });

    loop {
        tokio::select! {
            Some(msg) = rx.recv() => {
                if let Ok(bytes) = serde_json::to_vec(&msg) {
                    match swarm.behaviour_mut().gossipsub.publish(topic.clone(), bytes) {
                        Ok(_) => {}
                        Err(err) => {
                            match classify_publish_error(&err) {
                                PublishOutcome::StoredLocally => {
                                    let db_clone = Arc::clone(&db);
                                    let msg_clone = msg.clone();
                                    tokio::spawn(async move {
                                        persist_local_fallback(&db_clone, &msg_clone, port).await;
                                    });
                                }
                                PublishOutcome::Dropped => {
                                    eprintln!("❌ [Gossipsub - Port {}] Failed to publish message: {:?}", port, err);
                                }
                            }
                        }
                    }
                }
            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                    println!("🟢 [NETWORK - Port {}] Connection established with peer {} at {:?}", port, peer_id, endpoint);
                }
                SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                    println!("❌ [NETWORK - Port {}] Outgoing connection error to {:?}: {:?}", port, peer_id, error);
                }
                SwarmEvent::Behaviour(SwarmBehaviourStructEvent::Gossipsub(
                    libp2p::gossipsub::Event::Message { message: _message, .. }
                )) => {
                    if let Ok(swarm_msg) = serde_json::from_slice::<SwarmMessage>(&_message.data) {
                        let db_clone = Arc::clone(&db);

                        match swarm_msg {
                            SwarmMessage::FeedCard { id, title, url, summary, magnet_link, author_peer_id, timestamp } => {
                                println!("\n📰 [NODE {}] [FEED CARD RECEIVED from {}]", port, author_peer_id);
                                println!("  Title:   {}", title);
                                println!("  Summary: {}", summary);
                                if let Some(ref magnet) = magnet_link {
                                    println!("  🧲 Magnet: {}", magnet);
                                }

                                tokio::spawn(async move {
                                    // 1. Save feed card to SQLite
                                    let mut db = db_clone.lock().await;
                                    if let Err(e) = db.save_feed_card(&id, &author_peer_id, &title, &url, &summary, magnet_link.as_deref(), timestamp) {
                                        eprintln!("❌ Failed to save feed card to SQLite: {}", e);
                                    } else {
                                        println!("  💾 [DB - Port {}] Feed card persisted successfully.", port);
                                    }

                                    // 2. Magnet links are never downloaded automatically; the user chooses.
                                });
                            }
                            // Voting is retired: it generated too much traffic, so votes are ignored.
                            SwarmMessage::Vote { .. } => {}
                            SwarmMessage::DirectMessage { sender_peer_id, content, .. } => {
                                println!("\n💬 [NODE {}] [DM from {}]: {}", port, sender_peer_id, content);
                            }
                        }
                    }
                }
                SwarmEvent::Behaviour(SwarmBehaviourStructEvent::Mdns(libp2p::mdns::Event::Discovered(peers))) => {
                    for (peer_id, addr) in peers {
                        swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                        println!("🤝 [mDNS - Port {}] Discovered peer: {} at {}", port, peer_id, addr);
                        
                        if let Err(e) = swarm.dial(addr) {
                            println!("⚠️ [Port {}] Failed to dial peer {}: {:?}", port, peer_id, e);
                        }
                    }
                }
                SwarmEvent::Behaviour(SwarmBehaviourStructEvent::Mdns(libp2p::mdns::Event::Expired(peers))) => {
                    for (peer_id, _addr) in peers {
                        swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                        println!("🔌 [mDNS - Port {}] Peer expired: {}", port, peer_id);
                    }
                }
                _ => {}
            },
        }
    }
}
