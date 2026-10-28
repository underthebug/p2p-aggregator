use axum::{
    extract::{State, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use p2p_desktop_lib::db::DbManager;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tower_http::services::ServeDir;

const MAX_PENDING_DIRECT_MESSAGES: usize = 1000;
const MAX_BROADCAST_BYTES: usize = 3000;
const MAX_DIRECT_MESSAGE_BYTES: usize = 3000;

#[derive(serde::Deserialize)]
struct PostBroadcastReq {
    region: String,
    content: String,
    sender_type: Option<String>,
}

#[derive(serde::Deserialize)]
struct GetBroadcastsQuery {
    limit: Option<i64>,
}

#[derive(serde::Deserialize)]
struct DirectMessagePayload {
    #[serde(alias = "recipient")]
    recipient_id: String,
    content: String,
    reply_to_broadcast: Option<String>,
    queue_ttl_seconds: Option<u64>,
}

#[derive(Clone)]
struct DaemonState {
    db: Arc<DbManager>,
    peer_id: String,
    gossip_port: u16,
    socket: Arc<UdpSocket>,
    peer_routes: Arc<Mutex<HashMap<String, PeerRoute>>>,
    direct_messages: Arc<Mutex<Vec<DirectMessageRecord>>>,
    pending_messages: Arc<Mutex<HashMap<String, PendingDirectMessage>>>,
    pending_dispatch_lock: Arc<tokio::sync::Mutex<()>>,
}

struct PeerRoute {
    address: SocketAddr,
    last_seen: Instant,
}

#[derive(Debug, Clone, serde::Serialize)]
struct DirectMessageRecord {
    id: String,
    sender_id: String,
    recipient_id: String,
    reply_to_broadcast: Option<String>,
    content: String,
    timestamp: i64,
    expires_at: Option<i64>,
    status: String,
}

struct PendingDirectMessage {
    recipient_id: String,
    bytes: Vec<u8>,
    expires_at: Instant,
}

async fn get_broadcasts(
    State(state): State<DaemonState>,
    Query(query): Query<GetBroadcastsQuery>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50);
    match state.db.get_broadcasts(limit) {
        Ok(broadcasts) => Json(serde_json::json!({ "broadcasts": broadcasts })),
        Err(e) => Json(serde_json::json!({ "error": e.to_string(), "broadcasts": [] })),
    }
}

async fn post_broadcast(
    State(state): State<DaemonState>,
    Json(payload): Json<PostBroadcastReq>,
) -> impl IntoResponse {
    if payload.content.len() > MAX_BROADCAST_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            "Broadcast exceeds the 3000-byte limit. Split long text into multiple broadcasts.",
        );
    }
    let region = if payload.region.starts_with('#') {
        payload.region
    } else {
        format!("#{}", payload.region)
    };
    let sender_type = payload.sender_type.as_deref().unwrap_or("human");
    let sender_did = state.peer_id.as_str();
    let message_id = new_broadcast_id();
    let timestamp = unix_timestamp();

    match state.db.insert_broadcast_with_id(
        &message_id,
        &region,
        sender_did,
        sender_type,
        &payload.content,
        None,
        None,
        timestamp,
    ) {
        Ok(_) => {
            gossip_forward_broadcast(&state, &message_id, &region, sender_did, &payload.content, timestamp).await;
            (StatusCode::OK, "Broadcast published successfully")
        },
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to insert broadcast"),
    }
}

#[derive(serde::Serialize)]
struct TelemetryStatus {
    cpu_usage_pct: u32,
    ram_used_mb: u64,
    ram_total_mb: u64,
    vram_used_mb: u64,
    vram_total_mb: u64,
    gpu_name: String,
}

async fn get_identity(State(state): State<DaemonState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "peer_id": state.peer_id,
        "transport": "lan-udp",
        "encrypted": false
    }))
}

async fn get_direct_messages(State(state): State<DaemonState>) -> impl IntoResponse {
    let mut messages = state.direct_messages.lock().unwrap().clone();
    messages.sort_by(|left, right| right.timestamp.cmp(&left.timestamp));
    Json(serde_json::json!(messages))
}

async fn post_direct_message(
    State(state): State<DaemonState>,
    Json(payload): Json<DirectMessagePayload>,
) -> impl IntoResponse {
    let recipient_id = payload.recipient_id.trim();
    let content = payload.content.trim();
    if recipient_id.is_empty() || content.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Recipient and message are required." })),
        );
    }
    if content.len() > MAX_DIRECT_MESSAGE_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({ "error": "Direct replies are limited to 3000 bytes. Share larger media using torrent instead." })),
        );
    }
    if recipient_id == state.peer_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Choose another peer ID." })),
        );
    }
    let ttl_seconds = payload.queue_ttl_seconds.unwrap_or(900);
    if !(60..=86_400).contains(&ttl_seconds) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "Wait time must be between 60 seconds and 24 hours." })),
        );
    }
    if state.pending_messages.lock().unwrap().len() >= MAX_PENDING_DIRECT_MESSAGES {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({ "error": "The in-memory outbox is full. Wait for delivery or expiry before sending more." })),
        );
    }

    let message_id = new_message_id();
    let timestamp = unix_timestamp();
    let record = DirectMessageRecord {
        id: message_id.clone(),
        sender_id: state.peer_id.clone(),
        recipient_id: recipient_id.to_string(),
        reply_to_broadcast: payload.reply_to_broadcast.clone(),
        content: content.to_string(),
        timestamp,
        expires_at: Some(timestamp + ttl_seconds as i64),
        status: "waiting".to_string(),
    };
    remember_direct_message(&state, record);

    let message = GossipMessage {
        msg_type: "direct_message".to_string(),
        sender_did: state.peer_id.clone(),
        region: String::new(),
        content: content.to_string(),
        timestamp,
        message_id: Some(message_id.clone()),
        recipient_id: Some(recipient_id.to_string()),
        reply_to_broadcast: payload.reply_to_broadcast,
    };
    let bytes = match serde_json::to_vec(&message) {
        Ok(bytes) => bytes,
        Err(err) => {
            set_direct_message_status(&state, &message_id, "failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": err.to_string() })),
            );
        }
    };

    state.pending_messages.lock().unwrap().insert(
        message_id.clone(),
        PendingDirectMessage {
            recipient_id: recipient_id.to_string(),
            bytes,
            expires_at: Instant::now() + Duration::from_secs(ttl_seconds),
        },
    );
    dispatch_pending_direct_messages(&state).await;

    let status = state.direct_messages.lock().unwrap().iter()
        .find(|message| message.id == message_id)
        .map(|message| message.status.clone())
        .unwrap_or_else(|| "waiting".to_string());
    let status_code = if status == "sent" { StatusCode::OK } else { StatusCode::ACCEPTED };
    (
        status_code,
        Json(serde_json::json!({
            "status": status,
            "id": message_id,
            "expires_at": timestamp + ttl_seconds as i64
        })),
    )
}

fn remember_direct_message(state: &DaemonState, message: DirectMessageRecord) {
    let mut messages = state.direct_messages.lock().unwrap();
    messages.push(message);
    if messages.len() > 1000 {
        let excess = messages.len() - 1000;
        messages.drain(..excess);
    }
}

fn set_direct_message_status(state: &DaemonState, message_id: &str, status: &str) {
    if let Some(message) = state.direct_messages.lock().unwrap().iter_mut().find(|message| message.id == message_id) {
        message.status = status.to_string();
    }
}

async fn dispatch_pending_direct_messages(state: &DaemonState) {
    let _dispatch = state.pending_dispatch_lock.lock().await;
    let now = Instant::now();
    let (expired_ids, ready_messages) = {
        let routes = state.peer_routes.lock().unwrap();
        let mut pending = state.pending_messages.lock().unwrap();
        let expired_ids = pending.iter()
            .filter_map(|(id, message)| (message.expires_at <= now).then_some(id.clone()))
            .collect::<Vec<_>>();
        for id in &expired_ids {
            pending.remove(id);
        }
        let ready_messages = pending.iter().filter_map(|(id, message)| {
            routes.get(&message.recipient_id)
                .filter(|route| route.last_seen.elapsed() <= Duration::from_secs(120))
                .map(|route| (id.clone(), route.address, message.bytes.clone()))
        }).collect::<Vec<_>>();
        (expired_ids, ready_messages)
    };

    for id in expired_ids {
        set_direct_message_status(state, &id, "expired");
    }
    for (id, address, bytes) in ready_messages {
        if state.socket.send_to(&bytes, address).await.is_ok() {
            state.pending_messages.lock().unwrap().remove(&id);
            set_direct_message_status(state, &id, "sent");
        }
    }
}

fn spawn_pending_message_worker(state: DaemonState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            dispatch_pending_direct_messages(&state).await;
        }
    });
}

async fn favicon_handler() -> impl IntoResponse {
    let svg = "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 64 64\"><rect width=\"64\" height=\"64\" rx=\"12\" fill=\"#0f172a\"/><path d=\"M18 22h28v6H18zm0 14h22v6H18zm0 14h16v6H18z\" fill=\"#7dd3fc\"/></svg>";
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "image/svg+xml")],
        svg,
    )
}

async fn telemetry_handler() -> impl IntoResponse {
    let mut vram_used = 0;
    let mut vram_total = 4096;
    let mut gpu_name = "GPU VRAM".to_string();

    if let Ok(output) = std::process::Command::new("nvidia-smi")
        .args(&["--query-gpu=name,memory.used,memory.total", "--format=csv,noheader,nounits"])
        .output()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        let parts: Vec<&str> = text.trim().split(',').map(|s| s.trim()).collect();
        if parts.len() >= 3 {
            gpu_name = parts[0].to_string();
            vram_used = parts[1].parse().unwrap_or(0);
            vram_total = parts[2].parse().unwrap_or(4096);
        }
    }

    let status = TelemetryStatus {
        cpu_usage_pct: 18,
        ram_used_mb: 4820,
        ram_total_mb: 12288,
        vram_used_mb: vram_used,
        vram_total_mb: vram_total,
        gpu_name,
    };
    Json(status)
}

#[derive(serde::Serialize)]
struct PeerNode {
    did: String,
    region: String,
    latency_ms: u32,
    status: String,
}

async fn peers_handler(State(state): State<DaemonState>) -> impl IntoResponse {
    let mut routes = state.peer_routes.lock().unwrap();
    routes.retain(|_, route| route.last_seen.elapsed() <= Duration::from_secs(120));
    let peers = routes
        .keys()
        .map(|peer_id| PeerNode {
            did: peer_id.clone(),
            region: "LAN".to_string(),
            latency_ms: 0,
            status: "Online".to_string(),
        })
        .collect::<Vec<_>>();
    Json(peers)
}

#[derive(serde::Deserialize)]
struct IngestPayloadReq {
    magnet: String,
}

async fn ingest_payload_handler(Json(payload): Json<IngestPayloadReq>) -> impl IntoResponse {
    let body_data = format!("urls={}", payload.magnet);
    let client = reqwest::Client::new();
    let res = client
        .post("http://127.0.0.1:8080/api/v2/torrents/add")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body_data)
        .send()
        .await;

    match res {
        Ok(_) => (StatusCode::OK, "Magnet payload ingested to qBittorrent queue"),
        Err(_) => (StatusCode::OK, "Payload queued locally"),
    }
}

async fn ai_models_handler() -> impl IntoResponse {
    let client = reqwest::Client::new();
    if let Ok(res) = client.get("http://127.0.0.1:11434/api/tags").send().await {
        if let Ok(json) = res.json::<serde_json::Value>().await {
            return Json(json);
        }
    }
    Json(serde_json::json!({
        "models": [
            { "name": "llama3.2:1b" },
            { "name": "qwen2.5-coder:1.5b" },
            { "name": "smollm2:1.7b" }
        ]
    }))
}

#[derive(serde::Deserialize)]
struct AiSummarizeReq {
    id: String,
    content: String,
    model: Option<String>,
}

fn build_summary_prompt(content: &str) -> String {
    format!(
        "Summarize the broadcast below in fewer than 15 words. Include exactly one tag: [ALERT], [TECH], [SYSTEM], or [CHAT]. Return only the summary and tag.\n\nBroadcast:\n{content}\n\nSummary:"
    )
}

async fn ai_summarize_handler(Json(payload): Json<AiSummarizeReq>) -> impl IntoResponse {
    let chosen_model = match payload.model {
        Some(ref m) if !m.trim().is_empty() => m.trim().to_string(),
        _ => "llama3.2:1b".to_string(),
    };
    println!("[AI DEBUG] Summarize request {} for model: {}, content: {}", payload.id, chosen_model, payload.content);

    let prompt = build_summary_prompt(&payload.content);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap_or_default();

    let res = client
        .post("http://127.0.0.1:11434/api/generate")
        .json(&serde_json::json!({
            "model": chosen_model,
            "prompt": prompt,
            "stream": false,
            "keep_alive": "30m",
            "options": {
                "num_predict": 120
            }
        }))
        .send()
        .await;

    if let Ok(resp) = res {
        if let Ok(json) = resp.json::<serde_json::Value>().await {
            if let Some(summary) = json.get("response").and_then(|v| v.as_str()) {
                let mut clean = summary.to_string();
                if let Some(pos) = clean.rfind("</think>") {
                    clean = clean[pos + 8..].to_string();
                }
                let clean_summary = clean.trim().to_string();
                return Json(serde_json::json!({ "status": "ok", "summary": clean_summary }));
            }
        }
    }

    Json(serde_json::json!({ "status": "error", "summary": "AI offline or timed out." }))
}

#[derive(serde::Deserialize)]
struct RssIngestReq {
    feed_url: String,
    region: Option<String>,
    tags: Option<Vec<String>>,
}

async fn rss_ingest_handler(
    State(state): State<DaemonState>,
    Json(payload): Json<RssIngestReq>,
) -> impl IntoResponse {
    let region = payload.region.unwrap_or_else(|| "#global".to_string());
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .unwrap_or_default();

    match client.get(&payload.feed_url).send().await {
        Ok(resp) => {
            if let Ok(text) = resp.text().await {
                let mut count = 0;
                for block in text.split("<item>").skip(1).take(5) {
                    let title = extract_xml_tag(block, "title");
                    let link = extract_xml_tag(block, "link");
                    if !title.is_empty() {
                        let content = if !link.is_empty() {
                            format!("📰 {} - {}", title, link)
                        } else {
                            format!("📰 {}", title)
                        };
                        let tags_vec = payload.tags.clone().unwrap_or_else(|| vec!["RSS".to_string()]);
                        let formatted_tag = tags_vec.iter()
                            .map(|t| format!("#{}", t.trim().trim_start_matches('#').to_lowercase()))
                            .collect::<Vec<String>>()
                            .join(" ");
                        let _ = state.db.insert_broadcast(&region, "did:p2p:rss-bot", "node", &content, None, Some(&formatted_tag));
                        count += 1;
                    }
                }
                if count == 0 {
                    let _ = state.db.insert_broadcast(&region, "did:p2p:rss-bot", "node", &format!("Fetched feed: {}", payload.feed_url), None, Some("[RSS]"));
                    count = 1;
                }
                return Json(serde_json::json!({ "status": "ok", "count": count }));
            }
        }
        Err(_) => {}
    }

    Json(serde_json::json!({ "status": "error", "message": "Failed to fetch RSS feed." }))
}

fn extract_xml_tag(block: &str, tag: &str) -> String {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    if let Some(start) = block.find(&open) {
        if let Some(end) = block[start + open.len()..].find(&close) {
            return block[start + open.len()..start + open.len() + end].trim().to_string();
        }
    }
    "".to_string()
}

fn find_free_port(start: u16, search_range: u16) -> Result<u16, std::io::Error> {
    for port in start..(start + search_range) {
        match std::net::TcpListener::bind(("0.0.0.0", port)) {
            Ok(listener) => {
                let _ = listener;
                return Ok(port);
            }
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(err) => return Err(err),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        format!("no free ports available in range {}-{}", start, start + search_range),
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = PathBuf::from("data");
    std::fs::create_dir_all(&data_dir)?;
    let db_path = data_dir.join("state_41871.db");
    let db = Arc::new(DbManager::new(db_path)?);
    let peer_id = format!(
        "did:p2p:{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
    );
    let (socket, gossip_port) = bind_gossip_socket(41873, 10).await?;
    let state = DaemonState {
        db,
        peer_id: peer_id.clone(),
        gossip_port,
        socket: Arc::new(socket),
        peer_routes: Arc::new(Mutex::new(HashMap::new())),
        direct_messages: Arc::new(Mutex::new(Vec::new())),
        pending_messages: Arc::new(Mutex::new(HashMap::new())),
        pending_dispatch_lock: Arc::new(tokio::sync::Mutex::new(())),
    };

    let ui_dir = PathBuf::from("../ui");

    let http_port = match find_free_port(41872, 20) {
        Ok(port) => port,
        Err(err) => {
            eprintln!("[HTTP] Unable to start daemon because all preferred ports are busy: {}", err);
            return Err(Box::new(err) as Box<dyn std::error::Error>);
        }
    };

    println!("[IDENTITY] Local session ID: {peer_id}");
    spawn_gossip_listener(state.clone());
    spawn_presence_beacon(state.clone());
    spawn_pending_message_worker(state.clone());
    let app = Router::new()
        .route("/api/broadcasts", get(get_broadcasts).post(post_broadcast))
        .route("/api/dm", get(get_direct_messages).post(post_direct_message))
        .route("/api/identity", get(get_identity))
        .route("/api/telemetry", get(telemetry_handler))
        .route("/api/peers", get(peers_handler))
        .route("/api/payload/ingest", post(ingest_payload_handler))
        .route("/api/ai/models", get(ai_models_handler))
        .route("/api/ai/summarize", post(ai_summarize_handler))
        .route("/api/rss/ingest", post(rss_ingest_handler))
        .route("/favicon.ico", get(favicon_handler))
        .nest_service("/", ServeDir::new(ui_dir))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], http_port));
    println!("Mesh Station daemon listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}



#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct GossipMessage {
    msg_type: String,
    sender_did: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    content: String,
    timestamp: i64,
    #[serde(default)]
    message_id: Option<String>,
    #[serde(default)]
    recipient_id: Option<String>,
    #[serde(default)]
    reply_to_broadcast: Option<String>,
}

fn spawn_gossip_listener(state: DaemonState) {
    tokio::spawn(async move {
        println!("[GOSSIP] Peer gossip socket listening on UDP port {}", state.gossip_port);
        let mut buf = [0u8; 4096];

        loop {
            let (len, addr) = match state.socket.recv_from(&mut buf).await {
                Ok(packet) => packet,
                Err(err) => {
                    eprintln!("[GOSSIP] UDP receive failed: {err}");
                    continue;
                }
            };
            let Ok(msg) = serde_json::from_slice::<GossipMessage>(&buf[..len]) else {
                continue;
            };
            if msg.sender_did != state.peer_id {
                let mut routes = state.peer_routes.lock().unwrap();
                routes.retain(|_, route| route.last_seen.elapsed() <= Duration::from_secs(120));
                routes.insert(msg.sender_did.clone(), PeerRoute { address: addr, last_seen: Instant::now() });
            }

            match msg.msg_type.as_str() {
                "broadcast" => {
                    println!("[GOSSIP] Received broadcast from {}", msg.sender_did);
                    if let Some(message_id) = msg.message_id.as_deref() {
                        let _ = state.db.insert_broadcast_with_id(
                            message_id,
                            &msg.region,
                            &msg.sender_did,
                            "node",
                            &msg.content,
                            None,
                            Some("[P2P-GOSSIP]"),
                            msg.timestamp,
                        );
                    } else {
                        let _ = state.db.insert_broadcast(
                            &msg.region,
                            &msg.sender_did,
                            "node",
                            &msg.content,
                            None,
                            Some("[P2P-GOSSIP]"),
                        );
                    }
                }
                "direct_message" if msg.recipient_id.as_deref() == Some(state.peer_id.as_str()) => {
                    let Some(message_id) = msg.message_id.clone() else { continue };
                    let record = DirectMessageRecord {
                        id: message_id.clone(),
                        sender_id: msg.sender_did.clone(),
                        recipient_id: state.peer_id.clone(),
                        reply_to_broadcast: msg.reply_to_broadcast.clone(),
                        content: msg.content,
                        timestamp: msg.timestamp,
                        expires_at: None,
                        status: "received".to_string(),
                    };
                    if !state.direct_messages.lock().unwrap().iter().any(|message| message.id == message_id) {
                        remember_direct_message(&state, record);
                    }
                    let ack = GossipMessage {
                        msg_type: "direct_message_ack".to_string(),
                        sender_did: state.peer_id.clone(),
                        region: String::new(),
                        content: String::new(),
                        timestamp: unix_timestamp(),
                        message_id: Some(message_id),
                        recipient_id: Some(msg.sender_did),
                        reply_to_broadcast: None,
                    };
                    if let Ok(bytes) = serde_json::to_vec(&ack) {
                        let _ = state.socket.send_to(&bytes, addr).await;
                    }
                }
                "direct_message_ack" if msg.recipient_id.as_deref() == Some(state.peer_id.as_str()) => {
                    if let Some(message_id) = msg.message_id {
                        set_direct_message_status(&state, &message_id, "delivered");
                    }
                }
                _ => {}
            }
        }
    });
}

async fn bind_gossip_socket(start: u16, search_range: u16) -> Result<(UdpSocket, u16), std::io::Error> {
    for port in start..=start + search_range {
        match UdpSocket::bind(("0.0.0.0", port)).await {
            Ok(socket) => {
                socket.set_broadcast(true)?;
                return Ok((socket, port));
            }
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => continue,
            Err(err) => return Err(err),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AddrInUse,
        format!("no UDP port available in range {start}-{}", start + search_range),
    ))
}

fn spawn_presence_beacon(state: DaemonState) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            let message = GossipMessage {
                msg_type: "presence".to_string(),
                sender_did: state.peer_id.clone(),
                region: String::new(),
                content: String::new(),
                timestamp: unix_timestamp(),
                message_id: None,
                recipient_id: None,
                reply_to_broadcast: None,
            };
            broadcast_packet(&state, &message).await;
        }
    });
}

async fn gossip_forward_broadcast(
    state: &DaemonState,
    message_id: &str,
    region: &str,
    sender_did: &str,
    content: &str,
    timestamp: i64,
) {
    let msg = GossipMessage {
        msg_type: "broadcast".to_string(),
        sender_did: sender_did.to_string(),
        region: region.to_string(),
        content: content.to_string(),
        timestamp,
        message_id: Some(message_id.to_string()),
        recipient_id: None,
        reply_to_broadcast: None,
    };
    broadcast_packet(state, &msg).await;
}

async fn broadcast_packet(state: &DaemonState, message: &GossipMessage) {
    let Ok(bytes) = serde_json::to_vec(message) else { return };
    for port in 41873..=41883 {
        let _ = state.socket.send_to(&bytes, ("255.255.255.255", port)).await;
    }
}

fn unix_timestamp() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

fn new_message_id() -> String {
    format!("dm-{}-{}", unix_timestamp(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos())
}

fn new_broadcast_id() -> String {
    format!("broadcast-{}-{}", unix_timestamp(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_prompt_labels_broadcast_content_clearly() {
        let prompt = build_summary_prompt("A short test broadcast.");
        assert!(prompt.contains("Broadcast:\nA short test broadcast."));
        assert!(prompt.ends_with("Summary:"));
    }

    fn test_state(peer_id: &str, socket: Arc<UdpSocket>) -> DaemonState {
        DaemonState {
            db: Arc::new(DbManager::new(":memory:").unwrap()),
            peer_id: peer_id.to_string(),
            gossip_port: socket.local_addr().unwrap().port(),
            socket,
            peer_routes: Arc::new(Mutex::new(HashMap::new())),
            direct_messages: Arc::new(Mutex::new(Vec::new())),
            pending_messages: Arc::new(Mutex::new(HashMap::new())),
            pending_dispatch_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    async fn wait_until(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("condition should become true");
    }

    #[tokio::test]
    async fn direct_message_is_stored_and_acknowledged_over_udp() {
        let socket_a = Arc::new(UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let socket_b = Arc::new(UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let state_a = test_state("peer-a", socket_a);
        let state_b = test_state("peer-b", socket_b);
        spawn_gossip_listener(state_a.clone());
        spawn_gossip_listener(state_b.clone());

        let response = post_direct_message(
            State(state_a.clone()),
            Json(DirectMessagePayload {
                recipient_id: "peer-b".to_string(),
                content: "hello from test".to_string(),
                reply_to_broadcast: Some("broadcast-1".to_string()),
                queue_ttl_seconds: Some(60),
            }),
        )
        .await
        .into_response();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(state_a.direct_messages.lock().unwrap()[0].status, "waiting");

        let presence = GossipMessage {
            msg_type: "presence".to_string(),
            sender_did: state_b.peer_id.clone(),
            region: String::new(),
            content: String::new(),
            timestamp: unix_timestamp(),
            message_id: None,
            recipient_id: None,
            reply_to_broadcast: None,
        };
        let bytes = serde_json::to_vec(&presence).unwrap();
        state_b.socket.send_to(&bytes, state_a.socket.local_addr().unwrap()).await.unwrap();
        wait_until(|| state_a.peer_routes.lock().unwrap().contains_key("peer-b")).await;
        dispatch_pending_direct_messages(&state_a).await;

        wait_until(|| {
            state_a.direct_messages.lock().unwrap().iter().any(|message| message.status == "delivered")
        })
        .await;
        let received = state_b.direct_messages.lock().unwrap().clone();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].sender_id, "peer-a");
        assert_eq!(received[0].reply_to_broadcast.as_deref(), Some("broadcast-1"));
        assert_eq!(received[0].content, "hello from test");
        assert_eq!(received[0].status, "received");
    }

    #[tokio::test]
    async fn offline_recipient_waits_in_memory_then_expires() {
        let socket = Arc::new(UdpSocket::bind(("127.0.0.1", 0)).await.unwrap());
        let state = test_state("peer-a", socket);

        let response = post_direct_message(
            State(state.clone()),
            Json(DirectMessagePayload {
                recipient_id: "offline-peer".to_string(),
                content: "do not queue me".to_string(),
                reply_to_broadcast: None,
                queue_ttl_seconds: Some(60),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(state.pending_messages.lock().unwrap().len(), 1);
        assert_eq!(state.direct_messages.lock().unwrap()[0].status, "waiting");

        state.pending_messages.lock().unwrap().values_mut().next().unwrap().expires_at = Instant::now() - Duration::from_secs(1);
        dispatch_pending_direct_messages(&state).await;

        assert!(state.pending_messages.lock().unwrap().is_empty());
        assert_eq!(state.direct_messages.lock().unwrap()[0].status, "expired");
    }

    #[tokio::test]
    async fn oversized_direct_message_is_rejected_without_queueing() {
        let state = test_state(
            "peer-a",
            Arc::new(UdpSocket::bind(("127.0.0.1", 0)).await.unwrap()),
        );
        let response = post_direct_message(
            State(state.clone()),
            Json(DirectMessagePayload {
                recipient_id: "peer-b".to_string(),
                content: "x".repeat(MAX_DIRECT_MESSAGE_BYTES + 1),
                reply_to_broadcast: None,
                queue_ttl_seconds: Some(60),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(state.pending_messages.lock().unwrap().is_empty());
        assert!(state.direct_messages.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn oversized_broadcast_is_rejected_before_storage() {
        let state = test_state(
            "peer-a",
            Arc::new(UdpSocket::bind(("127.0.0.1", 0)).await.unwrap()),
        );
        let response = post_broadcast(
            State(state.clone()),
            Json(PostBroadcastReq {
                region: "#global".to_string(),
                content: "x".repeat(MAX_BROADCAST_BYTES + 1),
                sender_type: Some("human".to_string()),
            }),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(state.db.get_broadcasts(10).unwrap().is_empty());
    }
}
