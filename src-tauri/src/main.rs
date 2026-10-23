use libp2p::{identity, PeerId, SwarmBuilder, Multiaddr};
use libp2p::futures::StreamExt;
use libp2p::kad::store::MemoryStore;
use libp2p::kad::Behaviour as Kademlia;
use libp2p::mdns::{tokio::Behaviour as MdnsBehaviour, Event as MdnsEvent};
use libp2p::swarm::NetworkBehaviour;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, HashMap};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tauri::State;
use tokio::sync::mpsc;

#[derive(NetworkBehaviour)]
struct MeshBehaviour {
    kademlia: Kademlia<MemoryStore>,
    mdns: MdnsBehaviour,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NetworkPayload {
    pub action: String,
    pub post_id: String,
    pub title: String,
    pub target_url: String,
    pub topic: String,
    pub region: String,
    pub message_body: String,
    pub author_pubkey: String,
    pub timestamp: i64,
    pub delta: i32,
    pub signature: Vec<u8>,
    pub media_urls: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct FeedPost {
    pub id: String,
    pub author_pubkey: String,
    pub title: String,
    pub target_url: String,
    pub topic: String,
    pub region: String,
    pub votes: i32,
    pub message_body: String,
    pub media_urls: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppSettings {
    pub font_size: i32,
    pub theme: String,
    pub accent_color: String,
    pub card_capacity: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AiAgent {
    pub id: String,
    pub name: String,
    pub endpoint_url: String,
    pub model: String,
    pub prompt: String,
    pub interval_secs: u64,
    pub target_topic: String,
    pub status: String,
    pub last_run: i64,
    pub last_output: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FileShareItem {
    pub id: String,
    pub filename: String,
    pub magnet_link: String,
    pub file_size: String,
    pub uploader_pubkey: String,
    pub timestamp: i64,
}

pub struct Blocklist(pub Mutex<HashSet<String>>);
pub struct LocalIdentity(pub String);
pub struct ConnectedPeers(pub Arc<Mutex<HashMap<String, String>>>);

fn prune_old_cards(conn: &Connection, max_limit: i64) -> Result<(), String> {
    let _ = conn.execute(
        "DELETE FROM posts WHERE id NOT IN (
            SELECT id FROM posts ORDER BY timestamp DESC LIMIT ?1
        )",
        params![max_limit],
    );
    let _ = conn.execute(
        "DELETE FROM saved_posts WHERE id NOT IN (
            SELECT id FROM saved_posts ORDER BY saved_at DESC LIMIT 100
        )",
        [],
    );
    Ok(())
}

#[tauri::command]
fn get_app_settings() -> Result<AppSettings, String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT)", []);

    let get_val = |key: &str, default: &str| -> String {
        let mut stmt = conn.prepare("SELECT value FROM app_settings WHERE key = ?1").unwrap();
        let val: Result<String, _> = stmt.query_row(params![key], |row| row.get(0));
        val.unwrap_or_else(|_| default.to_string())
    };

    Ok(AppSettings {
        font_size: get_val("font_size", "14").parse().unwrap_or(14),
        theme: get_val("theme", "dark-cyberpunk"),
        accent_color: get_val("accent_color", "#00ffcc"),
        card_capacity: get_val("card_capacity", "25").parse().unwrap_or(25),
    })
}

#[tauri::command]
fn save_app_settings(font_size: i32, theme: String, accent_color: String, card_capacity: i32) -> Result<(), String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES ('font_size', ?1)", params![font_size.to_string()]);
    let _ = conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES ('theme', ?1)", params![theme]);
    let _ = conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES ('accent_color', ?1)", params![accent_color]);
    let _ = conn.execute("INSERT OR REPLACE INTO app_settings (key, value) VALUES ('card_capacity', ?1)", params![card_capacity.to_string()]);
    Ok(())
}

#[tauri::command]
fn get_ai_agents() -> Result<Vec<AiAgent>, String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute(
        "CREATE TABLE IF NOT EXISTS ai_agents (
            id TEXT PRIMARY KEY,
            name TEXT,
            endpoint_url TEXT,
            model TEXT,
            prompt TEXT,
            interval_secs INTEGER,
            target_topic TEXT,
            status TEXT,
            last_run INTEGER,
            last_output TEXT
        )",
        [],
    );

    let mut stmt = conn.prepare("SELECT id, name, endpoint_url, model, prompt, interval_secs, target_topic, status, last_run, COALESCE(last_output, '') FROM ai_agents").map_err(|e| e.to_string())?;
    let agents = stmt.query_map([], |row| {
        Ok(AiAgent {
            id: row.get(0)?,
            name: row.get(1)?,
            endpoint_url: row.get(2)?,
            model: row.get(3)?,
            prompt: row.get(4)?,
            interval_secs: row.get(5)?,
            target_topic: row.get(6)?,
            status: row.get(7)?,
            last_run: row.get(8)?,
            last_output: row.get(9)?,
        })
    }).map_err(|e| e.to_string())?
      .filter_map(|r| r.ok())
      .collect();

    Ok(agents)
}

#[tauri::command]
fn save_ai_agent(agent: AiAgent) -> Result<(), String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute(
        "INSERT OR REPLACE INTO ai_agents (id, name, endpoint_url, model, prompt, interval_secs, target_topic, status, last_run, last_output) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![agent.id, agent.name, agent.endpoint_url, agent.model, agent.prompt, agent.interval_secs, agent.target_topic, agent.status, agent.last_run, agent.last_output],
    );
    Ok(())
}

#[tauri::command]
fn delete_ai_agent(agent_id: String) -> Result<(), String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute("DELETE FROM ai_agents WHERE id = ?1", params![agent_id]);
    Ok(())
}

#[tauri::command]
fn get_file_shares() -> Result<Vec<FileShareItem>, String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute(
        "CREATE TABLE IF NOT EXISTS file_shares (
            id TEXT PRIMARY KEY,
            filename TEXT,
            magnet_link TEXT,
            file_size TEXT,
            uploader_pubkey TEXT,
            timestamp INTEGER
        )",
        [],
    );
    let mut stmt = conn.prepare("SELECT id, filename, magnet_link, file_size, uploader_pubkey, timestamp FROM file_shares ORDER BY timestamp DESC LIMIT 50").map_err(|e| e.to_string())?;
    let items = stmt.query_map([], |row| {
        Ok(FileShareItem {
            id: row.get(0)?,
            filename: row.get(1)?,
            magnet_link: row.get(2)?,
            file_size: row.get(3)?,
            uploader_pubkey: row.get(4)?,
            timestamp: row.get(5)?,
        })
    }).map_err(|e| e.to_string())?
      .filter_map(|r| r.ok())
      .collect();
    Ok(items)
}

#[tauri::command]
fn publish_file_share(filename: String, magnet_link: String, file_size: String, local_id: State<'_, LocalIdentity>) -> Result<(), String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let id = format!("file_{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let _ = conn.execute(
        "INSERT OR REPLACE INTO file_shares (id, filename, magnet_link, file_size, uploader_pubkey, timestamp) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, filename, magnet_link, file_size, local_id.0.clone(), now],
    );
    Ok(())
}

#[tauri::command]
async fn query_llm_inference(req: LlmQueryRequest) -> Result<LlmQueryResponse, String> {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(60)).build().map_err(|e| e.to_string())?;
    let target_url = if req.endpoint_url.trim().is_empty() { "http://localhost:11434/api/generate".to_string() } else { req.endpoint_url.trim().to_string() };

    let body = serde_json::json!({
        "model": if req.model.is_empty() { "llama3" } else { &req.model },
        "prompt": req.prompt,
        "stream": false
    });

    let resp = client.post(&target_url).json(&body).send().await.map_err(|e| format!("LLM connection failed: {}", e))?;
    let json_resp = resp.json::<serde_json::Value>().await.map_err(|e| format!("Failed to parse LLM response: {}", e))?;

    let answer = json_resp["response"].as_str()
        .or_else(|| json_resp["choices"][0]["text"].as_str())
        .unwrap_or("Received valid response but could not parse text field.")
        .to_string();

    Ok(LlmQueryResponse { response: answer })
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LlmQueryRequest {
    pub endpoint_url: String,
    pub model: String,
    pub prompt: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LlmQueryResponse {
    pub response: String,
}

#[tauri::command]
fn fetch_feed(limit: Option<i64>, active_region: Option<String>, feed_mode: Option<String>) -> Result<Vec<FeedPost>, String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let fetch_limit = limit.unwrap_or(25);

    let _ = conn.execute(
        "CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            author_pubkey TEXT,
            title TEXT,
            target_url TEXT,
            topic TEXT,
            region TEXT,
            is_media BOOLEAN,
            timestamp INTEGER,
            votes INTEGER,
            message_body TEXT,
            media_urls TEXT
        )",
        [],
    );

    let _ = conn.execute(
        "CREATE TABLE IF NOT EXISTS saved_posts (
            id TEXT PRIMARY KEY,
            author_pubkey TEXT,
            title TEXT,
            target_url TEXT,
            topic TEXT,
            region TEXT,
            is_media BOOLEAN,
            timestamp INTEGER,
            votes INTEGER,
            message_body TEXT,
            media_urls TEXT,
            saved_at INTEGER
        )",
        [],
    );

    let region_filter = active_region.unwrap_or_else(|| "global".to_string()).to_lowercase();
    let mode = feed_mode.unwrap_or_else(|| "live".to_string());

    let query = if mode == "live" {
        if region_filter == "global" || region_filter.is_empty() {
            "SELECT id, author_pubkey, title, target_url, topic, COALESCE(region, 'global'), votes, COALESCE(message_body, ''), COALESCE(media_urls, '') FROM posts WHERE topic NOT IN ('wiki-stream', 'tech-stream', 'code-stream', 'archive-stream') ORDER BY timestamp DESC LIMIT ?1".to_string()
        } else {
            format!("SELECT id, author_pubkey, title, target_url, topic, COALESCE(region, 'global'), votes, COALESCE(message_body, ''), COALESCE(media_urls, '') FROM posts WHERE (LOWER(region) = '{}') AND topic NOT IN ('wiki-stream', 'tech-stream', 'code-stream', 'archive-stream') ORDER BY timestamp DESC LIMIT ?1", region_filter)
        }
    } else if mode == "wiki-stream" {
        "SELECT id, author_pubkey, title, target_url, topic, COALESCE(region, 'global'), votes, COALESCE(message_body, ''), COALESCE(media_urls, '') FROM posts WHERE topic = 'wiki-stream' ORDER BY timestamp DESC LIMIT ?1".to_string()
    } else if mode == "tech-stream" {
        "SELECT id, author_pubkey, title, target_url, topic, COALESCE(region, 'global'), votes, COALESCE(message_body, ''), COALESCE(media_urls, '') FROM posts WHERE topic = 'tech-stream' ORDER BY timestamp DESC LIMIT ?1".to_string()
    } else if mode == "code-stream" {
        "SELECT id, author_pubkey, title, target_url, topic, COALESCE(region, 'global'), votes, COALESCE(message_body, ''), COALESCE(media_urls, '') FROM posts WHERE topic = 'code-stream' ORDER BY timestamp DESC LIMIT ?1".to_string()
    } else if mode == "archive-stream" {
        "SELECT id, author_pubkey, title, target_url, topic, COALESCE(region, 'global'), votes, COALESCE(message_body, ''), COALESCE(media_urls, '') FROM posts WHERE topic = 'archive-stream' ORDER BY timestamp DESC LIMIT ?1".to_string()
    } else {
        "SELECT id, author_pubkey, title, target_url, topic, COALESCE(region, 'global'), votes, COALESCE(message_body, ''), COALESCE(media_urls, '') FROM posts ORDER BY timestamp DESC LIMIT ?1".to_string()
    };

    let mut stmt = conn.prepare(&query).map_err(|e| e.to_string())?;
    let posts = stmt
        .query_map(params![fetch_limit], |row| {
            let media_raw: String = row.get(8)?;
            let media_urls: Vec<String> = if media_raw.trim().is_empty() { Vec::new() } else { media_raw.split(',').map(|s| s.trim().to_string()).collect() };

            Ok(FeedPost {
                id: row.get(0)?,
                author_pubkey: row.get(1)?,
                title: row.get(2)?,
                target_url: row.get(3)?,
                topic: row.get(4)?,
                region: row.get(5)?,
                votes: row.get(6)?,
                message_body: row.get(7)?,
                media_urls,
            })
        })
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .collect();

    Ok(posts)
}

#[tauri::command]
fn fetch_saved_feed() -> Result<Vec<FeedPost>, String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let mut stmt = conn.prepare("SELECT id, author_pubkey, title, target_url, topic, COALESCE(region, 'global'), votes, COALESCE(message_body, ''), COALESCE(media_urls, '') FROM saved_posts ORDER BY saved_at DESC LIMIT 100").map_err(|e| e.to_string())?;

    let posts = stmt
        .query_map([], |row| {
            let media_raw: String = row.get(8)?;
            let media_urls: Vec<String> = if media_raw.trim().is_empty() { Vec::new() } else { media_raw.split(',').map(|s| s.trim().to_string()).collect() };

            Ok(FeedPost {
                id: row.get(0)?,
                author_pubkey: row.get(1)?,
                title: row.get(2)?,
                target_url: row.get(3)?,
                topic: row.get(4)?,
                region: row.get(5)?,
                votes: row.get(6)?,
                message_body: row.get(7)?,
                media_urls,
            })
        })
        .map_err(|e| e.to_string())?
        .filter_map(|r| r.ok())
        .collect();

    Ok(posts)
}

#[tauri::command]
fn save_card_to_db(post_id: String) -> Result<(), String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let _ = conn.execute(
        "INSERT OR REPLACE INTO saved_posts (id, author_pubkey, title, target_url, topic, region, is_media, timestamp, votes, message_body, media_urls, saved_at)
         SELECT id, author_pubkey, title, target_url, topic, region, is_media, timestamp, votes, message_body, media_urls, ?1
         FROM posts WHERE id = ?2",
        params![now, post_id],
    );
    let _ = prune_old_cards(&conn, 25);
    Ok(())
}

#[tauri::command]
fn unsave_card_in_db(post_id: String) -> Result<(), String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute("DELETE FROM saved_posts WHERE id = ?1", params![post_id]);
    Ok(())
}

#[tauri::command]
fn create_post_from_ui(
    title: String,
    target_url: String,
    topic: String,
    region: String,
    media_urls: Vec<String>,
    local_id: State<'_, LocalIdentity>,
    tx: State<'_, mpsc::UnboundedSender<NetworkPayload>>,
) -> Result<String, String> {
    let clean_title = title.trim();
    if clean_title.is_empty() { return Err("Title cannot be empty.".to_string()); }

    let clean_topic = if topic.trim().is_empty() { "general".to_string() } else { topic.trim().to_lowercase() };
    let clean_region = if region.trim().is_empty() { "global".to_string() } else { region.trim().to_lowercase() };
    let post_id = format!("post_{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let joined_media = media_urls.join(",");
    let my_pubkey = &local_id.0;

    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute(
        "INSERT OR IGNORE INTO posts (id, author_pubkey, title, target_url, topic, region, is_media, timestamp, votes, message_body, media_urls) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![post_id, my_pubkey, clean_title, target_url, clean_topic, clean_region, !media_urls.is_empty(), now, 0, "", joined_media],
    );
    let _ = prune_old_cards(&conn, 100);

    let payload = NetworkPayload {
        action: "NEW_POST".to_string(),
        post_id: post_id.clone(),
        title: clean_title.to_string(),
        target_url,
        topic: clean_topic,
        region: clean_region,
        message_body: String::new(),
        author_pubkey: my_pubkey.clone(),
        timestamp: now,
        delta: 0,
        signature: Vec::new(),
        media_urls,
    };
    let _ = tx.send(payload);
    Ok(post_id)
}

#[tauri::command]
fn send_text_burst_from_ui(
    topic: String,
    region: String,
    message_body: String,
    local_id: State<'_, LocalIdentity>,
    tx: State<'_, mpsc::UnboundedSender<NetworkPayload>>,
) -> Result<String, String> {
    let clean_body = message_body.trim();
    if clean_body.is_empty() { return Err("Message body cannot be empty.".to_string()); }

    let clean_topic = if topic.trim().is_empty() { "general".to_string() } else { topic.trim().to_lowercase() };
    let clean_region = if region.trim().is_empty() { "global".to_string() } else { region.trim().to_lowercase() };
    let post_id = format!("burst_{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let my_pubkey = &local_id.0;

    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute(
        "INSERT OR IGNORE INTO posts (id, author_pubkey, title, target_url, topic, region, is_media, timestamp, votes, message_body, media_urls) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![post_id, my_pubkey, "Mesh Text Burst", "", clean_topic, clean_region, false, now, 0, clean_body, ""],
    );
    let _ = prune_old_cards(&conn, 100);

    let payload = NetworkPayload {
        action: "SHORT_TEXT".to_string(),
        post_id: post_id.clone(),
        title: "Mesh Text Burst".to_string(),
        target_url: String::new(),
        topic: clean_topic,
        region: clean_region,
        message_body: clean_body.to_string(),
        author_pubkey: my_pubkey.clone(),
        timestamp: now,
        delta: 0,
        signature: Vec::new(),
        media_urls: Vec::new(),
    };
    let _ = tx.send(payload);
    Ok(post_id)
}

#[tauri::command]
fn vote_from_ui(
    post_id: String,
    delta: i32,
    local_id: State<'_, LocalIdentity>,
    tx: State<'_, mpsc::UnboundedSender<NetworkPayload>>,
) -> Result<(), String> {
    let conn = Connection::open("/home/russ/p2p-aggregator/p2p_node.db").map_err(|e| e.to_string())?;
    let _ = conn.execute("UPDATE posts SET votes = votes + ?1 WHERE id = ?2", params![delta, post_id]);

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    let payload = NetworkPayload {
        action: "NEW_VOTE".to_string(),
        post_id,
        title: String::new(),
        target_url: String::new(),
        topic: String::new(),
        region: String::new(),
        message_body: String::new(),
        author_pubkey: local_id.0.clone(),
        timestamp: now,
        delta,
        signature: Vec::new(),
        media_urls: Vec::new(),
    };
    let _ = tx.send(payload);
    Ok(())
}

#[tauri::command]
fn get_connected_peers(peers: State<'_, ConnectedPeers>) -> Result<Vec<(String, String)>, String> {
    let map = peers.0.lock().map_err(|e| e.to_string())?;
    Ok(map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

fn spawn_multi_public_streams() {
    // Stream 1: Wikipedia Recent Changes
    std::thread::spawn(move || {
        let client = reqwest::blocking::Client::builder().timeout(Duration::from_secs(30)).build().unwrap();
        loop {
            let url = "https://en.wikipedia.org/w/api.php?action=query&list=recentchanges&rclimit=5&format=json";
            if let Ok(resp) = client.get(url).header("User-Agent", "P2PLinkAggregator/1.0").send() {
                if let Ok(json) = resp.json::<serde_json::Value>() {
                    if let Some(changes) = json["query"]["recentchanges"].as_array() {
                        for change in changes {
                            if let (Some(title), Some(pageid)) = (change["title"].as_str(), change["pageid"].as_i64()) {
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                                let post_id = format!("wiki_{}", pageid);
                                let target_url = format!("https://en.wikipedia.org/?curid={}", pageid);
                                if let Ok(conn) = Connection::open("/home/russ/p2p-aggregator/p2p_node.db") {
                                    let _ = conn.execute(
                                        "INSERT OR IGNORE INTO posts (id, author_pubkey, title, target_url, topic, region, is_media, timestamp, votes, message_body, media_urls) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                                        params![post_id, format!("wiki_peer_{}", pageid % 999), format!("[Wiki Stream] {}", title), target_url, "wiki-stream", "global", false, now, 1, "", ""],
                                    );
                                }
                            }
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(50));
        }
    });

    // Stream 2: Tech Stream (Hacker News)
    std::thread::spawn(move || {
        let client = reqwest::blocking::Client::builder().timeout(Duration::from_secs(30)).build().unwrap();
        loop {
            let url = "https://hacker-news.firebaseio.com/v0/topstories.json";
            if let Ok(resp) = client.get(url).header("User-Agent", "P2PLinkAggregator/1.0").send() {
                if let Ok(ids) = resp.json::<Vec<i64>>() {
                    for id in ids.iter().take(3) {
                        let item_url = format!("https://hacker-news.firebaseio.com/v0/item/{}.json", id);
                        if let Ok(item_resp) = client.get(&item_url).header("User-Agent", "P2PLinkAggregator/1.0").send() {
                            if let Ok(item_json) = item_resp.json::<serde_json::Value>() {
                                if let (Some(title), Some(item_url_val)) = (item_json["title"].as_str(), item_json["url"].as_str()) {
                                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                                    let post_id = format!("hn_{}", id);
                                    if let Ok(conn) = Connection::open("/home/russ/p2p-aggregator/p2p_node.db") {
                                        let _ = conn.execute(
                                            "INSERT OR IGNORE INTO posts (id, author_pubkey, title, target_url, topic, region, is_media, timestamp, votes, message_body, media_urls) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                                            params![post_id, format!("hn_peer_{}", id % 999), format!("[Tech Stream] {}", title), item_url_val, "tech-stream", "global", false, now, 2, "", ""],
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(90));
        }
    });

    // Stream 3: Code Stream (Crates.io Registry Search API)
    std::thread::spawn(move || {
        let client = reqwest::blocking::Client::builder().timeout(Duration::from_secs(30)).build().unwrap();
        loop {
            let url = "https://crates.io/api/v1/crates?per_page=5&sort=downloads";
            if let Ok(resp) = client.get(url).header("User-Agent", "P2PLinkAggregator/1.0 (russ@p2p)").send() {
                if let Ok(json) = resp.json::<serde_json::Value>() {
                    if let Some(crates) = json["crates"].as_array() {
                        for krate in crates {
                            if let Some(name) = krate["name"].as_str() {
                                let desc = krate["description"].as_str().unwrap_or("Rust package");
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                                let post_id = format!("crate_{}", name);
                                let target_url = format!("https://crates.io/crates/{}", name);
                                if let Ok(conn) = Connection::open("/home/russ/p2p-aggregator/p2p_node.db") {
                                    let _ = conn.execute(
                                        "INSERT OR IGNORE INTO posts (id, author_pubkey, title, target_url, topic, region, is_media, timestamp, votes, message_body, media_urls) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                                        params![post_id, format!("crate_peer_{}", &name[..2.min(name.len())]), format!("[Code Stream] {} - {}", name, desc), target_url, "code-stream", "global", false, now, 3, "", ""],
                                    );
                                }
                            }
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(60));
        }
    });

    // Stream 4: Internet Archive Stream
    std::thread::spawn(move || {
        let client = reqwest::blocking::Client::builder().timeout(Duration::from_secs(30)).build().unwrap();
        loop {
            let url = "https://archive.org/advancedsearch.php?q=mediatype:texts&sort=-publicdate&rows=5&output=json";
            if let Ok(resp) = client.get(url).header("User-Agent", "P2PLinkAggregator/1.0 (russ@p2p)").send() {
                if let Ok(json) = resp.json::<serde_json::Value>() {
                    if let Some(docs) = json["response"]["docs"].as_array() {
                        for doc in docs {
                            if let (Some(identifier), Some(title)) = (doc["identifier"].as_str(), doc["title"].as_str()) {
                                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
                                let post_id = format!("archive_{}", identifier);
                                let target_url = format!("https://archive.org/details/{}", identifier);
                                if let Ok(conn) = Connection::open("/home/russ/p2p-aggregator/p2p_node.db") {
                                    let _ = conn.execute(
                                        "INSERT OR IGNORE INTO posts (id, author_pubkey, title, target_url, topic, region, is_media, timestamp, votes, message_body, media_urls) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                                        params![post_id, format!("archive_peer_{}", &identifier[..4.min(identifier.len())]), format!("[Archive Stream] {}", title), target_url, "archive-stream", "global", false, now, 4, "", ""],
                                    );
                                }
                            }
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(60));
        }
    });
}

async fn run_p2p_node(
    mut rx: mpsc::UnboundedReceiver<NetworkPayload>, 
    local_key: identity::Keypair,
    connected_peers: Arc<Mutex<HashMap<String, String>>>
) -> Result<(), Box<dyn Error>> {
    let _local_peer_id = PeerId::from(local_key.public());
    
    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(libp2p::tcp::Config::default(), libp2p::noise::Config::new, libp2p::yamux::Config::default)?
        .with_behaviour(|key| {
            let peer_id = PeerId::from(key.public());
            let mut kademlia = Kademlia::new(peer_id, MemoryStore::new(peer_id));
            
            let bootstrap_nodes = vec![
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
                "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
            ];

            for addr_str in bootstrap_nodes {
                if let Ok(addr) = addr_str.parse::<Multiaddr>() {
                    if let Some(libp2p::multiaddr::Protocol::P2p(peer)) = addr.iter().find(|p| matches!(p, libp2p::multiaddr::Protocol::P2p(_))) {
                        kademlia.add_address(&peer, addr);
                    }
                }
            }
            let _ = kademlia.bootstrap();
            let mdns = MdnsBehaviour::new(libp2p::mdns::Config::default(), peer_id).unwrap();
            MeshBehaviour { kademlia, mdns }
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let listen_addr: libp2p::Multiaddr = "/ip4/0.0.0.0/tcp/4001".parse()?;
    swarm.listen_on(listen_addr)?;

    loop {
        tokio::select! {
            Some(_payload) = rx.recv() => {},
            event = swarm.select_next_some() => {
                match event {
                    libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                        let addr_str = endpoint.get_remote_address().to_string();
                        if let Ok(mut map) = connected_peers.lock() { map.insert(peer_id.to_string(), addr_str); }
                    }
                    libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, .. } => {
                        if let Ok(mut map) = connected_peers.lock() { map.remove(&peer_id.to_string()); }
                    }
                    libp2p::swarm::SwarmEvent::Behaviour(MeshBehaviourEvent::Mdns(MdnsEvent::Discovered(list))) => {
                        for (peer_id, addr) in list {
                            swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
                            if let Ok(mut map) = connected_peers.lock() { map.insert(peer_id.to_string(), addr.to_string()); }
                        }
                    }
                    libp2p::swarm::SwarmEvent::Behaviour(MeshBehaviourEvent::Mdns(MdnsEvent::Expired(list))) => {
                        for (peer_id, _addr) in list {
                            if let Ok(mut map) = connected_peers.lock() { map.remove(&peer_id.to_string()); }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id_str = PeerId::from(local_key.public()).to_string();

    let mut bad_domains = HashSet::new();
    bad_domains.insert("example.com".to_string());

    let (tx, rx) = mpsc::unbounded_channel::<NetworkPayload>();
    let connected_peers = Arc::new(Mutex::new(HashMap::new()));
    let peers_state = ConnectedPeers(connected_peers.clone());

    let net_key = local_key.clone();
    tokio::spawn(async move {
        if let Err(e) = run_p2p_node(rx, net_key, connected_peers).await { eprintln!("P2P Node Error: {:?}", e); }
    });

    spawn_multi_public_streams();

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(Blocklist(Mutex::new(bad_domains)))
        .manage(LocalIdentity(local_peer_id_str))
        .manage(peers_state)
        .manage(tx)
        .invoke_handler(tauri::generate_handler![
            fetch_feed,
            fetch_saved_feed,
            save_card_to_db,
            unsave_card_in_db,
            vote_from_ui,
            create_post_from_ui,
            send_text_burst_from_ui,
            get_connected_peers,
            get_app_settings,
            save_app_settings,
            get_ai_agents,
            save_ai_agent,
            delete_ai_agent,
            query_llm_inference,
            get_file_shares,
            publish_file_share
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
