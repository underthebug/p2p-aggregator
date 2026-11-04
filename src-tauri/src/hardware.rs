// CPU, RAM, and GPU usage for the machine this process runs on.
use serde::Serialize;
use std::time::Duration;

#[derive(Serialize)]
pub struct Telemetry {
    pub cpu_usage_pct: u32,
    pub ram_used_mb: u64,
    pub ram_total_mb: u64,
    pub vram_used_mb: u64,
    pub vram_total_mb: u64,
    pub gpu_name: String,
}

pub async fn snapshot() -> Telemetry {
    let before = cpu_times();
    tokio::time::sleep(Duration::from_millis(250)).await;
    let after = cpu_times();

    let cpu_usage_pct = match (before, after) {
        (Some((idle0, total0)), Some((idle1, total1))) if total1 > total0 => {
            let delta_total = total1 - total0;
            let delta_busy = delta_total - (idle1 - idle0);
            (delta_busy * 100 / delta_total) as u32
        }
        _ => 0,
    };

    let (ram_used_mb, ram_total_mb) = memory_mb();
    let (gpu_name, vram_used_mb, vram_total_mb) = gpu();

    Telemetry {
        cpu_usage_pct,
        ram_used_mb,
        ram_total_mb,
        vram_used_mb,
        vram_total_mb,
        gpu_name,
    }
}

// Returns (idle, total) jiffies from the aggregate "cpu" line of /proc/stat.
fn cpu_times() -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let fields: Vec<u64> = stat
        .lines()
        .next()?
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    if fields.len() < 5 {
        return None;
    }
    // idle + iowait
    let idle = fields[3] + fields[4];
    Some((idle, fields.iter().sum()))
}

// Returns (used, total) in MB from /proc/meminfo.
fn memory_mb() -> (u64, u64) {
    let text = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let read_kb = |key: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(key))
            .and_then(|rest| rest.split_whitespace().next())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let total = read_kb("MemTotal:");
    let available = read_kb("MemAvailable:");
    (total.saturating_sub(available) / 1024, total / 1024)
}

// Returns (name, used MB, total MB) for the first NVIDIA GPU, or placeholders if unavailable.
fn gpu() -> (String, u64, u64) {
    let fallback = ("GPU VRAM".to_string(), 0, 4096);
    let Ok(output) = std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=name,memory.used,memory.total", "--format=csv,noheader,nounits"])
        .output()
    else {
        return fallback;
    };
    let text = String::from_utf8_lossy(&output.stdout);
    let first_line = text.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split(',').map(str::trim).collect();
    if parts.len() < 3 {
        return fallback;
    }
    (
        parts[0].to_string(),
        parts[1].parse().unwrap_or(0),
        parts[2].parse().unwrap_or(4096),
    )
}
