use std::sync::Arc;
use std::time::Duration;
use rusqlite::Connection;
use std::sync::Mutex;

pub struct TelemetryMetrics {
    pub cpu_usage: Mutex<f32>,
    pub memory_usage: Mutex<u64>,
    pub active_connections: Mutex<usize>,
    db_path: String,
    start_time: std::time::Instant,
}

impl TelemetryMetrics {
    pub fn new(db_path: &str) -> Arc<Self> {
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

        Arc::new(Self {
            cpu_usage: Mutex::new(0.0),
            memory_usage: Mutex::new(0),
            active_connections: Mutex::new(0),
            db_path: telemetry_db_path,
            start_time: std::time::Instant::now(),
        })
    }

    pub fn update(&self, cpu: f32, memory: u64, connections: usize) {
        *self.cpu_usage.lock().unwrap() = cpu;
        *self.memory_usage.lock().unwrap() = memory;
        *self.active_connections.lock().unwrap() = connections;

        let db_path = self.db_path.clone();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let _ = std::thread::spawn(move || {
            if let Ok(conn) = Connection::open(db_path) {
                let _ = conn.execute(
                    "INSERT OR REPLACE INTO metrics_log (timestamp, cpu, memory) VALUES (?1, ?2, ?3)",
                    rusqlite::params![ts, cpu, memory as i64],
                );
            }
        });
    }

    pub fn get_summary(&self) -> String {
        format!("CPU: {:.2}%, Memory: {} bytes, Active Connections: {}", 
            *self.cpu_usage.lock().unwrap(), 
            *self.memory_usage.lock().unwrap(), 
            *self.active_connections.lock().unwrap()
        )
    }

    pub fn get_raw_metrics(&self) -> (u64, u64) {
        let uptime = self.start_time.elapsed().as_secs();
        let mem = *self.memory_usage.lock().unwrap();
        (uptime, mem)
    }
}

pub fn start_telemetry_worker(metrics: Arc<TelemetryMetrics>) {
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(5));
            let cpu = 1.25;
            let mem = 1024 * 1024 * 16;
            let conn_count = 0;

            metrics.update(cpu, mem, conn_count);
        }
    });
}
