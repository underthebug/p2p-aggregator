//! Feed Monitor: keyword and sender alerts over configured feeds and the mesh ledger.
//! Shared by the desktop app and the browser daemon.

use crate::db::DbManager;
use crate::rss::{fetch_feed_text, is_http_url, parse_feed_items};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const POLL_SECS: u64 = 300;
/// Relative to the working directory, same as the rest of the app's data files.
pub const CONFIG_PATH: &str = "data/monitor.json";
const MAX_ALERTS: usize = 200;
const MAX_SEEN: usize = 5000;
const MAX_LIST_ENTRIES: usize = 50;
const MAX_ENTRY_CHARS: usize = 200;
const MESH_SCAN_LIMIT: i64 = 200;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MonitorConfig {
    #[serde(default)]
    pub feeds: Vec<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default)]
    pub sender_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MonitorAlert {
    pub id: u64,
    pub timestamp: i64,
    pub source: String,
    pub matched_on: String,
    pub matched_value: String,
    pub title: String,
    pub link: Option<String>,
    pub sender_id: Option<String>,
}

#[derive(Default)]
pub struct BoundedSeen {
    set: HashSet<String>,
    order: VecDeque<String>,
}

impl BoundedSeen {
    /// Returns true when the key was not seen before.
    pub fn insert(&mut self, key: String) -> bool {
        if self.set.contains(&key) {
            return false;
        }
        if self.order.len() >= MAX_SEEN {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        self.set.insert(key.clone());
        self.order.push_back(key);
        true
    }
}

#[derive(Default)]
pub struct MonitorState {
    pub config: MonitorConfig,
    pub alerts: VecDeque<MonitorAlert>,
    pub seen: BoundedSeen,
    pub next_alert_id: u64,
    pub seeded: bool,
    pub last_run: Option<i64>,
}

pub fn push_alert(state: &mut MonitorState, mut alert: MonitorAlert) {
    state.next_alert_id += 1;
    alert.id = state.next_alert_id;
    state.alerts.push_front(alert);
    while state.alerts.len() > MAX_ALERTS {
        state.alerts.pop_back();
    }
}

/// Trims, dedupes, and validates user-supplied rules.
pub fn normalize_config(raw: MonitorConfig) -> Result<MonitorConfig, String> {
    fn clean(list: Vec<String>, label: &str) -> Result<Vec<String>, String> {
        let mut out: Vec<String> = Vec::new();
        for item in list {
            let value = item.trim().to_string();
            if value.is_empty() || out.contains(&value) {
                continue;
            }
            if value.chars().count() > MAX_ENTRY_CHARS {
                return Err(format!("{} entries must be {} characters or fewer.", label, MAX_ENTRY_CHARS));
            }
            out.push(value);
        }
        if out.len() > MAX_LIST_ENTRIES {
            return Err(format!("{} limit is {} entries.", label, MAX_LIST_ENTRIES));
        }
        Ok(out)
    }

    let feeds = clean(raw.feeds, "Feed")?;
    if let Some(bad) = feeds.iter().find(|url| !is_http_url(url.as_str())) {
        return Err(format!("Feed URL must start with http:// or https://: {}", bad));
    }
    Ok(MonitorConfig {
        feeds,
        keywords: clean(raw.keywords, "Keyword")?,
        sender_ids: clean(raw.sender_ids, "Sender ID")?,
    })
}

/// Loads the saved rules. Missing or invalid files fall back to empty rules.
pub fn load_config(path: &Path) -> MonitorConfig {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str::<MonitorConfig>(&text).ok())
        .and_then(|config| normalize_config(config).ok())
        .unwrap_or_default()
}

/// Writes the rules atomically (temp file, then rename).
pub fn save_config(path: &Path, config: &MonitorConfig) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let json = serde_json::to_string_pretty(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let tmp_path = path.with_extension("json.tmp");
    std::fs::write(&tmp_path, json)?;
    std::fs::rename(&tmp_path, path)
}

/// Returns the first configured keyword found in the haystack (case-insensitive).
pub fn match_keyword<'a>(keywords: &'a [String], haystack: &str) -> Option<&'a str> {
    let lowered = haystack.to_lowercase();
    keywords
        .iter()
        .find(|keyword| lowered.contains(&keyword.to_lowercase()))
        .map(|keyword| keyword.as_str())
}

/// JSON view used by the UI: rules, alerts, poll interval, and last check time.
pub fn snapshot(state: &MonitorState) -> serde_json::Value {
    serde_json::json!({
        "config": state.config,
        "alerts": state.alerts,
        "poll_secs": POLL_SECS,
        "last_run": state.last_run,
    })
}

fn unix_timestamp() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

/// One pass over all configured feeds and the mesh ledger.
/// The first pass only records what already exists so startup does not flood alerts.
pub async fn run_cycle(db: &DbManager, monitor: &Mutex<MonitorState>) {
    let (config, alerts_enabled) = {
        let state = monitor.lock().unwrap();
        (state.config.clone(), state.seeded)
    };

    for feed_url in &config.feeds {
        let text = match fetch_feed_text(feed_url).await {
            Ok(text) => text,
            Err(_) => continue,
        };
        let Ok(items) = parse_feed_items(&text) else { continue };

        let mut state = monitor.lock().unwrap();
        for item in items {
            let identity = if item.link.is_empty() { item.title.clone() } else { item.link.clone() };
            if !state.seen.insert(format!("feed:{}|{}", feed_url, identity)) || !alerts_enabled {
                continue;
            }
            let haystack = format!("{} {}", item.title, item.link);
            if let Some(keyword) = match_keyword(&config.keywords, &haystack) {
                let alert = MonitorAlert {
                    id: 0,
                    timestamp: unix_timestamp(),
                    source: "feed".to_string(),
                    matched_on: "keyword".to_string(),
                    matched_value: keyword.to_string(),
                    title: item.title,
                    link: if item.link.is_empty() { None } else { Some(item.link) },
                    sender_id: None,
                };
                push_alert(&mut state, alert);
            }
        }
    }

    if let Ok(broadcasts) = db.get_broadcasts(MESH_SCAN_LIMIT) {
        let mut state = monitor.lock().unwrap();
        for broadcast in broadcasts {
            if !state.seen.insert(format!("mesh:{}", broadcast.id)) || !alerts_enabled {
                continue;
            }
            let sender_match = config
                .sender_ids
                .iter()
                .find(|id| id.as_str() == broadcast.sender_did)
                .map(|id| ("sender", id.clone()));
            let matched = sender_match.or_else(|| {
                match_keyword(&config.keywords, &broadcast.content)
                    .map(|keyword| ("keyword", keyword.to_string()))
            });
            if let Some((matched_on, matched_value)) = matched {
                let title: String = broadcast.content.chars().take(160).collect();
                let alert = MonitorAlert {
                    id: 0,
                    timestamp: broadcast.timestamp,
                    source: "mesh".to_string(),
                    matched_on: matched_on.to_string(),
                    matched_value,
                    title,
                    link: None,
                    sender_id: Some(broadcast.sender_did.clone()),
                };
                push_alert(&mut state, alert);
            }
        }
    }

    let mut state = monitor.lock().unwrap();
    state.seeded = true;
    state.last_run = Some(unix_timestamp());
}

/// Runs the monitor forever, once per `POLL_SECS`. Spawn on the app's async runtime.
pub async fn run_loop(db: Arc<DbManager>, monitor: Arc<Mutex<MonitorState>>) {
    loop {
        run_cycle(&db, &monitor).await;
        tokio::time::sleep(Duration::from_secs(POLL_SECS)).await;
    }
}
