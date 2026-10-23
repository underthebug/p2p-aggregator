use std::str::FromStr;

pub enum IpcCommand {
    Status,
    Peers,
    Metrics,
    Connect(String),
    LogLevel(String),
    Unknown(String),
}

impl FromStr for IpcCommand {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed == "status" {
            Ok(IpcCommand::Status)
        } else if trimmed == "peers" {
            Ok(IpcCommand::Peers)
        } else if trimmed == "metrics" {
            Ok(IpcCommand::Metrics)
        } else if let Some(addr) = trimmed.strip_prefix("connect ") {
            Ok(IpcCommand::Connect(addr.trim().to_string()))
        } else if let Some(level) = trimmed.strip_prefix("loglevel ") {
            Ok(IpcCommand::LogLevel(level.trim().to_string()))
        } else {
            Ok(IpcCommand::Unknown(trimmed.to_string()))
        }
    }
}
