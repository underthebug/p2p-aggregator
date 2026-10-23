use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use rusqlite::Connection;

pub struct TelemetryMetrics {
    pub cpu_usage: f32,
    pub memory_usage: u64,
    pub active_connections: usize,
    db_path: String,
}

impl TelemetryMetrics {
    pub fn new(db_path: &str) -> Arc<Mutex<Self>> {
        let telemetry_db_path = format!("{}_telemetry", db_path);
        if let Some(parent) = std::path::Path::new(&telemetry_db_path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let conn = Connection::open(&telemetry_db_path).expect("Failed to open telemetry db");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS metrics_log (
                timestamp INTEGER PRIMARY KEY,
                cpu REAL NOT NULL,
                memory INTEGER NOT NULL
            )",
            [],
        ).unwrap();

        Arc::new(Mutex::new(Self {
            cpu_usage: 0.0,
            memory_usage: 0,
            active_connections: 0,
            db_path: telemetry_db_path,
        }))
    }

    pub async fn update(&mut self, cpu: f32, memory: u64, connections: usize) {
        self.cpu_usage = cpu;
        self.memory_usage = memory;
        self.active_connections = connections;

        let db_path = self.db_path.clone();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let _ = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(db_path).unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO metrics_log (timestamp, cpu, memory) VALUES (?1, ?2, ?3)",
                rusqlite::params![ts, cpu, memory as i64],
            ).unwrap();
        }).await;
    }

    pub fn get_summary(&self) -> String {
        format!("CPU: {:.2}%, Memory: {} bytes, Active Connections: {}", self.cpu_usage, self.memory_usage, self.active_connections)
    }
}

pub fn start_telemetry_worker(metrics: Arc<Mutex<TelemetryMetrics>>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            // Collect mock/real system stats
            let cpu = 1.25;
            let mem = 1024 * 1024 * 16;
            let conn_count = 0;

            let mut m = metrics.lock().await;
            m.update(cpu, mem, conn_count).await;
        }
    });
}
