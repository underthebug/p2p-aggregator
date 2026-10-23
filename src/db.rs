use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Post {
    pub id: String,
    pub author_pubkey: String,
    pub title: String,
    pub target_url: String,
    pub is_media: bool,
    pub timestamp: i64,
    pub votes: i32,
    pub score: f64,
}

#[derive(Clone)]
pub struct Storage {
    conn: Arc<Mutex<Connection>>,
}

impl Storage {
    pub fn init() -> Result<Self> {
        let conn = Connection::open("p2p_node.db")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS posts (
                id TEXT PRIMARY KEY,
                author_pubkey TEXT NOT NULL,
                title TEXT NOT NULL,
                target_url TEXT NOT NULL,
                is_media BOOLEAN NOT NULL,
                timestamp INTEGER NOT NULL,
                votes INTEGER NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS voter_history (
                post_id TEXT NOT NULL,
                voter_pubkey TEXT NOT NULL,
                PRIMARY KEY (post_id, voter_pubkey)
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS votes (
                post_id TEXT NOT NULL,
                voter_pubkey TEXT NOT NULL,
                delta INTEGER NOT NULL,
                PRIMARY KEY (post_id, voter_pubkey)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS trust_matrix (
                truster_pubkey TEXT NOT NULL,
                trustee_pubkey TEXT NOT NULL,
                weight REAL NOT NULL,
                PRIMARY KEY (truster_pubkey, trustee_pubkey)
            )",
            [],
        )?;

        Ok(Storage {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn insert_post(&self, post: &Post) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO posts (id, author_pubkey, title, target_url, is_media, timestamp, votes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                post.id,
                post.author_pubkey,
                post.title,
                post.target_url,
                post.is_media,
                post.timestamp,
                post.votes
            ],
        )?;
        Ok(())
    }

pub fn vote_post(&self, post_id: &str, delta: i32, voter_pubkey: &str) -> Result<bool> {
    // Clamp any raw delta strictly to +1 or -1
    let clamped_delta = match delta {
        d if d > 0 => 1,
        d if d < 0 => -1,
        _ => 0,
    };

    if clamped_delta == 0 {
        return Ok(false);
    }

    let conn = self.conn.lock().unwrap();

    // Check if this voter pubkey has already recorded a vote for this post_id
    let mut stmt = conn.prepare("SELECT 1 FROM voter_history WHERE post_id = ?1 AND voter_pubkey = ?2")?;
    let exists = stmt.exists(params![post_id, voter_pubkey])?;

    if exists {
        println!(">>> Duplicate vote ignored for pubkey {}", voter_pubkey);
        return Ok(false);
    }

    // Insert vote record into history table
    conn.execute(
        "INSERT INTO voter_history (post_id, voter_pubkey) VALUES (?1, ?2)",
        params![post_id, voter_pubkey],
    )?;

    // Apply clamped delta update to posts table
    conn.execute(
        "UPDATE posts SET votes = votes + ?1 WHERE id = ?2",
        params![clamped_delta, post_id],
    )?;

    Ok(true)
}

    pub fn set_trust(&self, truster: &str, trustee: &str, weight: f64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO trust_matrix (truster_pubkey, trustee_pubkey, weight)
             VALUES (?1, ?2, ?3)",
            params![truster, trustee, weight],
        )?;
        Ok(())
    }

    pub fn get_ranked_posts(&self, local_pubkey: &str) -> Result<Vec<Post>> {
        let conn = self.conn.lock().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let mut stmt = conn.prepare(
            "SELECT id, author_pubkey, title, target_url, is_media, timestamp, votes FROM posts",
        )?;

        let post_iter = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let author_pubkey: String = row.get(1)?;
            let title: String = row.get(2)?;
            let target_url: String = row.get(3)?;
            let is_media: bool = row.get(4)?;
            let timestamp: i64 = row.get(5)?;
            let votes: i32 = row.get(6)?;

            let hours_elapsed = ((now - timestamp) as f64) / 3600.0;
            let base_score = ((votes as f64) - 1.0) / (hours_elapsed + 2.0).powf(1.8);

            let trust_multiplier: f64 = conn
                .query_row(
                    "SELECT weight FROM trust_matrix WHERE truster_pubkey = ?1 AND trustee_pubkey = ?2",
                    params![local_pubkey, author_pubkey],
                    |r| r.get(0),
                )
                .unwrap_or(1.0);

            let final_score = base_score * trust_multiplier.max(0.1);

            Ok(Post {
                id,
                author_pubkey,
                title,
                target_url,
                is_media,
                timestamp,
                votes,
                score: final_score,
            })
        })?;

        let mut posts = Vec::new();
        for post in post_iter {
            if let Ok(p) = post {
                posts.push(p);
            }
        }

        posts.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        Ok(posts)
    }
}
