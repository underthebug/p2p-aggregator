// CPU, RAM, and GPU usage for the machine this process runs on.
use serde::Serialize;
use std::time::Duration;
use sysinfo::System;

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
    // CPU usage needs two samples taken some time apart.
    let mut system = System::new();
    system.refresh_cpu();
    tokio::time::sleep(Duration::from_millis(250)).await;
    system.refresh_cpu();
    system.refresh_memory();

    let cpu_usage_pct = system.global_cpu_info().cpu_usage().round().clamp(0.0, 100.0) as u32;
    let ram_total_mb = system.total_memory() / (1024 * 1024);
    let ram_used_mb = system.used_memory() / (1024 * 1024);
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

// Returns (name, used MB, total MB) for the first NVIDIA GPU, or placeholders if unavailable.
fn gpu() -> (String, u64, u64) {
    let fallback = ("GPU VRAM".to_string(), 0, 4096);
    let mut command = std::process::Command::new("nvidia-smi");
    command.args(["--query-gpu=name,memory.used,memory.total", "--format=csv,noheader,nounits"]);
    let Ok(output) = crate::platform::hide_window(&mut command).output() else {
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
