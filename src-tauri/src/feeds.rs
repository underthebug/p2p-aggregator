use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

#[derive(Serialize, Deserialize, Debug)]
pub struct Feed {
    pub id: i32,
    pub name: String,
    pub url: String,
}

pub fn init_db(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS user_feeds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE
        )",
        [],
    )?;
    Ok(())
}

#[tauri::command]
pub fn get_user_feeds(state: tauri::State<'_, Mutex<Connection>>) -> Result<Vec<Feed>, String> {
    let conn = state.lock().map_err(|e| e.to_string())?;
    let mut stmt = conn.prepare("SELECT id, name, url FROM user_feeds").map_err(|e| e.to_string())?;
    let feed_iter = stmt.query_map([], |row| {
        Ok(Feed {
            id: row.get(0)?,
            name: row.get(1)?,
            url: row.get(2)?,
        })
    }).map_err(|e| e.to_string())?;

    let mut feeds = Vec::new();
    for feed in feed_iter {
        feeds.push(feed.unwrap());
    }
    Ok(feeds)
}

#[tauri::command]
pub fn add_user_feed(state: tauri::State<'_, Mutex<Connection>>, name: String, url: String) -> Result<(), String> {
    let conn = state.lock().map_err(|e| e.to_string())?;
    conn.execute(
        "INSERT OR REPLACE INTO user_feeds (name, url) VALUES (?1, ?2)",
        rusqlite::params![name, url],
    ).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
pub fn remove_user_feed(state: tauri::State<'_, Mutex<Connection>>, id: i32) -> Result<(), String> {
    let conn = state.lock().map_err(|e| e.to_string())?;
    conn.execute("DELETE FROM user_feeds WHERE id = ?1", rusqlite::params![id])
        .map_err(|e| e.to_string())?;
    Ok(())
}
