use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap},
    Terminal,
};
use rusqlite::Connection;
use ed25519_dalek::{SigningKey, Signer};
use rand::rngs::OsRng;
use std::{
    io,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone)]
pub struct BroadcastItem {
    pub id: String,
    pub region: String,
    pub sender_did: String,
    pub sender_type: String,
    pub content: String,
    pub magnet_link: Option<String>,
    pub signature_hex: String,
    pub timestamp: i64,
    pub is_verified: bool,
    pub is_subscribed: bool,
}

#[derive(PartialEq)]
enum AppState {
    Normal,
    Composing,
}

struct ComposeForm {
    region: String,
    sender_type: String,
    title: String,
    content: String,
    magnet: String,
    active_field: usize,
}

impl Default for ComposeForm {
    fn default() -> Self {
        Self {
            region: String::from("#pottstown"),
            sender_type: String::from("human"),
            title: String::from("Mesh Broadcast"),
            content: String::new(),
            magnet: String::new(),
            active_field: 0,
        }
    }
}

fn wrap_text(text: &str, max_width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    for raw_line in text.lines() {
        if raw_line.is_empty() {
            lines.push(String::new());
            continue;
        }
        let mut current_line = String::new();
        for word in raw_line.split_whitespace() {
            if current_line.chars().count() + word.chars().count() + 1 > max_width {
                if !current_line.is_empty() {
                    lines.push(current_line);
                    current_line = String::new();
                }
                if word.chars().count() > max_width {
                    let mut chunk = String::new();
                    for c in word.chars() {
                        chunk.push(c);
                        if chunk.chars().count() == max_width {
                            lines.push(chunk);
                            chunk = String::new();
                        }
                    }
                    if !chunk.is_empty() {
                        current_line = chunk;
                    }
                } else {
                    current_line = word.to_string();
                }
            } else {
                if !current_line.is_empty() {
                    current_line.push(' ');
                }
                current_line.push_str(word);
            }
        }
        if !current_line.is_empty() {
            lines.push(current_line);
        }
    }
    if lines.is_empty() && !text.is_empty() {
        lines.push(text.to_string());
    }
    lines
}

fn cull_old_discovered_regions(conn: &Connection) {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let cutoff = now.saturating_sub(86400);
    let _ = conn.execute(
        "DELETE FROM secure_broadcasts WHERE timestamp < ?1 AND region IN (SELECT region FROM regions WHERE is_subscribed = 0)",
        [cutoff],
    );
    let _ = conn.execute(
        "DELETE FROM regions WHERE last_seen < ?1 AND is_subscribed = 0",
        [cutoff],
    );
}

fn seed_and_sync_regions(conn: &Connection) {
    let _ = conn.execute(
        "CREATE TABLE IF NOT EXISTS regions (
            region TEXT PRIMARY KEY,
            is_subscribed BOOLEAN DEFAULT 0,
            last_seen INTEGER
        )",
        [],
    );
    let _ = conn.execute("ALTER TABLE regions RENAME COLUMN regain TO region", []);

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;

    let default_subscribed = vec!["#global", "#pottstown", "#tech"];
    for reg in default_subscribed {
        let _ = conn.execute(
            "INSERT OR IGNORE INTO regions (region, is_subscribed, last_seen) VALUES (?1, 1, ?2)",
            rusqlite::params![reg, now],
        );
    }

    let _ = conn.execute(
        "INSERT OR IGNORE INTO regions (region, is_subscribed, last_seen) 
         SELECT DISTINCT region, 0, ?1 FROM secure_broadcasts",
        [now],
    );

    let _ = conn.execute(
        "UPDATE regions SET last_seen = ?1 WHERE region IN (SELECT DISTINCT region FROM secure_broadcasts)",
        [now],
    );

    cull_old_discovered_regions(conn);
}

fn fetch_broadcasts(db_path: &str) -> (Vec<BroadcastItem>, Vec<(String, bool)>, Vec<(String, String)>) {
    let mut broadcasts = Vec::new();
    let mut regions_list = vec![(String::from("[All Frequencies]"), true)];
    let mut downloads = Vec::new();

    if let Ok(conn) = Connection::open(db_path) {
        let _ = conn.execute(
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
        );
        let _ = conn.execute("ALTER TABLE secure_broadcasts RENAME COLUMN regain TO region", []);
        let _ = conn.execute(
            "CREATE TABLE IF NOT EXISTS downloads (magnet_link TEXT PRIMARY KEY, title TEXT, status TEXT)",
            [],
        );

        seed_and_sync_regions(&conn);

        if let Ok(mut stmt) = conn.prepare("SELECT region, is_subscribed FROM regions ORDER BY is_subscribed DESC, last_seen DESC") {
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0).unwrap_or_else(|_| String::from("#unknown")),
                    row.get::<_, i32>(1).unwrap_or(0) == 1,
                ))
            });
            if let Ok(iter) = rows {
                for r in iter.flatten() {
                    regions_list.push(r);
                }
            }
        }

        let stmt = conn.prepare(
            "SELECT b.id, b.region, b.sender_did, b.sender_type, b.content, b.magnet_link, b.signature_hex, b.timestamp, COALESCE(r.is_subscribed, 0)
             FROM secure_broadcasts b
             LEFT JOIN regions r ON b.region = r.region
             ORDER BY b.timestamp DESC LIMIT 50"
        );

        if let Ok(mut s) = stmt {
            let rows = s.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0).unwrap_or_else(|_| String::from("unknown-id")),
                    row.get::<_, String>(1).unwrap_or_else(|_| String::from("#global")),
                    row.get::<_, String>(2).unwrap_or_else(|_| String::from("did:p2p:unknown")),
                    row.get::<_, String>(3).unwrap_or_else(|_| String::from("human")),
                    row.get::<_, String>(4).unwrap_or_else(|_| String::from("")),
                    row.get::<_, Option<String>>(5).unwrap_or(None),
                    row.get::<_, String>(6).unwrap_or_else(|_| String::from("")),
                    row.get::<_, i64>(7).unwrap_or(0),
                    row.get::<_, i32>(8).unwrap_or(0) == 1,
                ))
            });

            if let Ok(iter) = rows {
                for r in iter.flatten() {
                    let (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp, is_subscribed) = r;
                    let is_verified = sender_did.starts_with("did:p2p:");

                    broadcasts.push(BroadcastItem {
                        id,
                        region,
                        sender_did,
                        sender_type,
                        content,
                        magnet_link,
                        signature_hex,
                        timestamp,
                        is_verified,
                        is_subscribed,
                    });
                }
            }
        }

        if let Ok(mut d_stmt) = conn.prepare("SELECT title, status FROM downloads") {
            let d_rows = d_stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0).unwrap_or_else(|_| String::from("Untitled")),
                    row.get::<_, String>(1).unwrap_or_else(|_| String::from("Pending")),
                ))
            });
            if let Ok(iter) = d_rows {
                for r in iter.flatten() {
                    downloads.push(r);
                }
            }
        }
    }
    (broadcasts, regions_list, downloads)
}

fn save_signed_broadcast(db_path: &str, form: &ComposeForm) -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open(db_path)?;
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

    let mut csprng = OsRng;
    let signing_key = SigningKey::generate(&mut csprng);
    let verifying_key = signing_key.verifying_key();
    let sender_did = format!("did:p2p:{}", hex::encode(verifying_key.to_bytes()));

    let id = uuid::Uuid::new_v4().to_string();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let magnet_val = if form.magnet.trim().is_empty() { None } else { Some(form.magnet.clone()) };

    let payload_data = format!("{}:{}:{}:{}:{}:{:?}", id, form.region, sender_did, form.content, timestamp, magnet_val);
    let signature = signing_key.sign(payload_data.as_bytes());
    let signature_hex = hex::encode(signature.to_bytes());

    conn.execute(
        "INSERT INTO secure_broadcasts (id, region, sender_did, sender_type, content, magnet_link, signature_hex, timestamp) 
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params![
            id,
            form.region,
            sender_did,
            form.sender_type,
            form.content,
            magnet_val,
            signature_hex,
            timestamp
        ],
    )?;

    Ok(())
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let port = args.iter().position(|r| r == "--port").and_then(|i| args.get(i + 1)).unwrap_or(&"41871".to_string()).clone();
    let db_path = format!("./data/state_{}.db", port);

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let tick_rate = Duration::from_millis(250);
    let mut last_tick = Instant::now();

    let mut list_state = ListState::default();
    list_state.select(Some(0));

    let mut inspector_scroll: usize = 0;
    let mut tuned_region_idx = 0;
    let mut app_state = AppState::Normal;
    let mut compose_form = ComposeForm::default();
    let mut status_message = String::from("Use [Up/Down] to navigate feed, [PageUp/PageDown] to scroll inspector, [C] to compose, [Q] to quit.");

    loop {
        let (all_broadcasts, available_regions, active_downloads) = fetch_broadcasts(&db_path);

        if tuned_region_idx >= available_regions.len() {
            tuned_region_idx = 0;
        }
        let (current_region, is_sub) = available_regions
            .get(tuned_region_idx)
            .cloned()
            .unwrap_or((String::from("[All Frequencies]"), true));

        let filtered_broadcasts: Vec<BroadcastItem> = if current_region == "[All Frequencies]" {
            all_broadcasts
        } else {
            all_broadcasts.into_iter().filter(|b| b.region == current_region).collect()
        };

        if !filtered_broadcasts.is_empty() {
            if let Some(selected) = list_state.selected() {
                if selected >= filtered_broadcasts.len() {
                    list_state.select(Some(filtered_broadcasts.len() - 1));
                }
            } else {
                list_state.select(Some(0));
            }
        }

        terminal.draw(|f| {
            let size = f.size();
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(10), Constraint::Length(3)])
                .split(size);

            let sub_badge = if current_region == "[All Frequencies]" { "GLOBAL" } else if is_sub { "SUBSCRIBED" } else { "DISCOVERED" };
            let header = Paragraph::new(Line::from(vec![
                Span::styled(" P2P RADIO MESH ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
                Span::raw(format!("| Port: {} | Tuned: {} [{}] ({}/{}) ", port, current_region, sub_badge, tuned_region_idx + 1, available_regions.len())),
            ]))
            .block(Block::default().borders(Borders::ALL).title("Status"));
            f.render_widget(header, chunks[0]);

            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[1]);

            let items: Vec<ListItem> = if filtered_broadcasts.is_empty() {
                vec![ListItem::new("No broadcasts in this region. Press [C] to compose one.")]
            } else {
                filtered_broadcasts
                    .iter()
                    .map(|b| {
                        let sender_badge = if b.sender_type == "human" { "👤 Human" } else { "🤖 AI" };
                        let verify_badge = if b.is_verified { "[VERIFIED ✓]" } else { "[UNVERIFIED ✗]" };
                        let type_tag = if b.is_subscribed { "" } else { " [DISCOVERED]" };

                        let preview_content = if b.content.chars().count() > 60 {
                            format!("{}...", b.content.chars().take(57).collect::<String>())
                        } else {
                            b.content.clone()
                        };

                        let item_lines = vec![
                            Line::from(vec![
                                Span::styled(format!("[{}{}] {} ", b.region, type_tag, sender_badge), Style::default().fg(Color::Cyan)),
                                Span::styled(verify_badge, Style::default().fg(if b.is_verified { Color::Green } else { Color::Red }).add_modifier(Modifier::BOLD)),
                            ]),
                            Line::from(vec![Span::raw(format!("  {}", preview_content))]),
                        ];

                        ListItem::new(item_lines)
                    })
                    .collect()
            };

            let broadcast_list = List::new(items)
                .block(Block::default().borders(Borders::ALL).title("Radio Transmissions ([Up/Down] Select, [C] Compose)"))
                .highlight_style(Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD))
                .highlight_symbol(">> ");
            
            f.render_stateful_widget(broadcast_list, body_chunks[0], &mut list_state);

            let mut right_lines = vec![
                Line::from(Span::styled("Sovereign Card Inspector:", Style::default().add_modifier(Modifier::BOLD))),
                Line::from("--------------------------------------------------"),
            ];

            if let Some(selected_idx) = list_state.selected() {
                if let Some(b) = filtered_broadcasts.get(selected_idx) {
                    right_lines.push(Line::from(vec![Span::styled("ID: ", Style::default().fg(Color::Cyan)), Span::raw(&b.id)]));
                    right_lines.push(Line::from(vec![Span::styled("Region: ", Style::default().fg(Color::Cyan)), Span::raw(&b.region)]));
                    right_lines.push(Line::from(vec![Span::styled("Channel Type: ", Style::default().fg(Color::Cyan)), Span::raw(if b.is_subscribed { "Subscribed" } else { "Discovered" })]));
                    let short_did = if b.sender_did.len() > 25 { &b.sender_did[..25] } else { &b.sender_did };
                    right_lines.push(Line::from(vec![Span::styled("Sender DID: ", Style::default().fg(Color::Cyan)), Span::raw(format!("{}...", short_did))]));
                    right_lines.push(Line::from(vec![Span::styled("Crypto Status: ", Style::default().fg(Color::Cyan)), Span::styled(if b.is_verified { "VALID Signature ✓" } else { "UNVERIFIED ✗" }, Style::default().fg(if b.is_verified { Color::Green } else { Color::Red }).add_modifier(Modifier::BOLD))]));
                    right_lines.push(Line::from(""));
                    right_lines.push(Line::from(Span::styled("Full Message Content (Scroll with PageUp/PageDown):", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))));
                    right_lines.push(Line::from("--------------------------------------------------"));
                    for wrapped_line in wrap_text(&b.content, 44) {
                        right_lines.push(Line::from(vec![Span::raw(wrapped_line)]));
                    }
                } else {
                    right_lines.push(Line::from("No transmission selected."));
                }
            } else {
                right_lines.push(Line::from("No transmission selected."));
            }

            right_lines.push(Line::from(""));
            right_lines.push(Line::from(Span::styled("Active Downloads Queue:", Style::default().add_modifier(Modifier::BOLD))));
            right_lines.push(Line::from("--------------------------------------------------"));

            if active_downloads.is_empty() {
                right_lines.push(Line::from("No active downloads."));
            } else {
                for (d_title, status) in active_downloads {
                    right_lines.push(Line::from(vec![
                        Span::styled(format!("• {} ", d_title), Style::default().fg(Color::Green)),
                        Span::styled(format!("[{}]", status), Style::default().fg(Color::Yellow)),
                    ]));
                }
            }

            let right_panel = Paragraph::new(right_lines)
                .block(Block::default().borders(Borders::ALL).title("Inspector & Downloads ([PgUp/PgDown] Scroll)"))
                .scroll((inspector_scroll as u16, 0));
            f.render_widget(right_panel, body_chunks[1]);

            let footer = Paragraph::new(status_message.as_str())
                .style(Style::default().fg(Color::Yellow))
                .block(Block::default().borders(Borders::ALL).title("Controls"));
            f.render_widget(footer, chunks[2]);

            if app_state == AppState::Composing {
                let area = centered_rect(70, 65, size);
                f.render_widget(Clear, area);

                let f0_style = if compose_form.active_field == 0 { Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD) } else { Style::default() };
                let f1_style = if compose_form.active_field == 1 { Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD) } else { Style::default() };
                let f2_style = if compose_form.active_field == 2 { Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD) } else { Style::default() };
                let f3_style = if compose_form.active_field == 3 { Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD) } else { Style::default() };
                let f4_style = if compose_form.active_field == 4 { Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD) } else { Style::default() };

                let mut popup_lines = vec![
                    Line::from(vec![Span::styled(format!("1. Region: {}", compose_form.region), f0_style)]),
                    Line::from(vec![Span::styled(format!("2. Sender Type (human/ai): {}", compose_form.sender_type), f1_style)]),
                    Line::from(vec![Span::styled(format!("3. Title: {}", compose_form.title), f2_style)]),
                    Line::from(Span::styled("4. Message Body (Multi-line supported):", f3_style)),
                ];

                for content_line in compose_form.content.lines() {
                    popup_lines.push(Line::from(vec![Span::raw(format!("   {}", content_line))]));
                }
                if compose_form.content.is_empty() {
                    popup_lines.push(Line::from(vec![Span::styled("   (Type or paste your message/JSON here)", Style::default().fg(Color::DarkGray))]));
                }

                popup_lines.push(Line::from(vec![Span::styled(format!("5. Magnet Link (optional): {}", compose_form.magnet), f4_style)]));
                popup_lines.push(Line::from(""));
                popup_lines.push(Line::from(Span::styled("[Tab / Up / Down] Switch Field | [Enter] Newline (in body) | [Ctrl+S] Broadcast | [Esc] Cancel", Style::default().fg(Color::Cyan))));

                let popup = Paragraph::new(popup_lines)
                    .block(Block::default().borders(Borders::ALL).title("Compose Signed Radio Broadcast (Sub-Window Editor)"))
                    .style(Style::default().bg(Color::DarkGray))
                    .wrap(Wrap { trim: false });
                f.render_widget(popup, area);
            }
        })?;

        let timeout = tick_rate.checked_sub(last_tick.elapsed()).unwrap_or_else(|| Duration::from_secs(0));

        if crossterm::event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                match app_state {
                    AppState::Normal => {
                        match key.code {
                            KeyCode::Char('q') | KeyCode::Char('Q') => break,
                            KeyCode::Char('c') | KeyCode::Char('C') => {
                                app_state = AppState::Composing;
                                compose_form = ComposeForm::default();
                                status_message = String::from("✍️ Composing broadcast... Press [Esc] to cancel.");
                            }
                            KeyCode::Char('r') | KeyCode::Char('R') => {
                                if !available_regions.is_empty() {
                                    tuned_region_idx = (tuned_region_idx + 1) % available_regions.len();
                                    if let Some((reg, sub)) = available_regions.get(tuned_region_idx) {
                                        status_message = format!("📻 Tuned radio region to: {} [{}]", reg, if *sub { "SUBSCRIBED" } else { "DISCOVERED" });
                                    }
                                }
                            }
                            KeyCode::PageUp => {
                                inspector_scroll = inspector_scroll.saturating_sub(3);
                            }
                            KeyCode::PageDown => {
                                inspector_scroll += 3;
                            }
                            KeyCode::Down => {
                                if !filtered_broadcasts.is_empty() {
                                    let i = match list_state.selected() {
                                        Some(i) => if i >= filtered_broadcasts.len() - 1 { 0 } else { i + 1 },
                                        None => 0,
                                    };
                                    list_state.select(Some(i));
                                    inspector_scroll = 0; // Reset scroll on card change
                                }
                            }
                            KeyCode::Up => {
                                if !filtered_broadcasts.is_empty() {
                                    let i = match list_state.selected() {
                                        Some(i) => if i == 0 { filtered_broadcasts.len() - 1 } else { i - 1 },
                                        None => 0,
                                    };
                                    list_state.select(Some(i));
                                    inspector_scroll = 0; // Reset scroll on card change
                                }
                            }
                            _ => {}
                        }
                    }
                    AppState::Composing => {
                        match (key.modifiers, key.code) {
                            (KeyModifiers::CONTROL, KeyCode::Char('s')) | (KeyModifiers::CONTROL, KeyCode::Char('S')) => {
                                if save_signed_broadcast(&db_path, &compose_form).is_ok() {
                                    status_message = format!("🚀 Cryptographically signed broadcast published to [{}]!", compose_form.region);
                                } else {
                                    status_message = String::from("❌ Error publishing broadcast.");
                                }
                                app_state = AppState::Normal;
                            }
                            (_, KeyCode::Esc) => {
                                app_state = AppState::Normal;
                                status_message = String::from("❌ Broadcast composition cancelled.");
                            }
                            (_, KeyCode::Tab) => {
                                compose_form.active_field = (compose_form.active_field + 1) % 5;
                            }
                            (_, KeyCode::Down) => {
                                compose_form.active_field = (compose_form.active_field + 1) % 5;
                            }
                            (_, KeyCode::Up) => {
                                if compose_form.active_field == 0 {
                                    compose_form.active_field = 4;
                                } else {
                                    compose_form.active_field -= 1;
                                }
                            }
                            (_, KeyCode::Enter) => {
                                if compose_form.active_field == 3 {
                                    compose_form.content.push('\n');
                                } else {
                                    compose_form.active_field = (compose_form.active_field + 1) % 5;
                                }
                            }
                            (_, KeyCode::Backspace) => {
                                match compose_form.active_field {
                                    0 => { compose_form.region.pop(); }
                                    1 => { compose_form.sender_type.pop(); }
                                    2 => { compose_form.title.pop(); }
                                    3 => { compose_form.content.pop(); }
                                    4 => { compose_form.magnet.pop(); }
                                    _ => {}
                                }
                            }
                            (_, KeyCode::Char(c)) => {
                                match compose_form.active_field {
                                    0 => compose_form.region.push(c),
                                    1 => compose_form.sender_type.push(c),
                                    2 => compose_form.title.push(c),
                                    3 => compose_form.content.push(c),
                                    4 => compose_form.magnet.push(c),
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}
