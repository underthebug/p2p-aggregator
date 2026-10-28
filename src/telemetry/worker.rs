#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TelemetryMetrics {
    pub active_peers: usize,
    pub messages_sent: u64,
    pub messages_received: u64,
}

impl TelemetryMetrics {
    pub fn get_summary(&self) -> String {
        format!("Active Peers: {}, Sent: {}, Received: {}", self.active_peers, self.messages_sent, self.messages_received)
    }

    pub fn get_raw_metrics(&self) -> (u64, u64) {
        (0, self.messages_sent + self.messages_received)
    }
}

pub struct TelemetryWorker;
