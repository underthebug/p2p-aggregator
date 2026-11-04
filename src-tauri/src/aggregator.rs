// Control of the user-level systemd units that run the aggregator nodes.
use std::collections::BTreeMap;
use std::process::{Command, Output};

pub const UNITS: [&str; 3] = ["p2p-desktop-node1", "p2p-desktop-node2", "p2p-desktop-node3"];

/// Runs `start`, `restart`, or `stop` on all units.
pub fn control(action: &str) -> Result<(), String> {
    let verb = match action {
        "start" | "restart" | "stop" => action,
        _ => return Err(format!("unknown action: {action}")),
    };
    let reload = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .output()
        .map_err(|err| err.to_string())?;
    if !reload.status.success() {
        return Err(String::from_utf8_lossy(&reload.stderr).trim().to_string());
    }
    if verb != "stop" {
        // Clears start-limit failures so a crashed unit can be started again.
        let _ = Command::new("systemctl")
            .args(["--user", "reset-failed"])
            .args(UNITS)
            .output();
    }
    let out = Command::new("systemctl")
        .arg("--user")
        .arg(verb)
        .args(UNITS)
        .output()
        .map_err(|err| err.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Maps each unit to its `systemctl is-active` state.
pub fn status() -> BTreeMap<String, String> {
    UNITS
        .iter()
        .map(|&unit| {
            let state = is_active(unit).unwrap_or_else(|| "unknown".to_string());
            (unit.to_string(), state)
        })
        .collect()
}

fn is_active(unit: &str) -> Option<String> {
    let out: Output = Command::new("systemctl")
        .args(["--user", "is-active", unit])
        .output()
        .ok()?;
    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
}
