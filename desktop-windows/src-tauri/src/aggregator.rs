// Runs the three aggregator nodes as child processes (Windows has no systemd).
use crate::platform;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;

pub const UNITS: [&str; 3] = ["p2p-desktop-node1", "p2p-desktop-node2", "p2p-desktop-node3"];

const NODE_BINARY: &str = "p2p-aggregator.exe";
const BOOTSTRAP: &str = "/ip4/127.0.0.1/tcp/41871";
// Port and optional bootstrap peer for each unit; nodes 2 and 3 dial node 1.
const NODE_ARGS: [(&str, Option<&str>); 3] = [
    ("41871", None),
    ("41872", Some(BOOTSTRAP)),
    ("41873", Some(BOOTSTRAP)),
];

static NODES: Mutex<[Option<Child>; 3]> = Mutex::new([None, None, None]);

/// Runs `start`, `restart`, or `stop` on all nodes.
pub fn control(action: &str) -> Result<(), String> {
    let mut nodes = NODES.lock().map_err(|err| err.to_string())?;
    match action {
        "start" => start_all(&mut nodes),
        "stop" => {
            stop_all(&mut nodes);
            Ok(())
        }
        "restart" => {
            stop_all(&mut nodes);
            start_all(&mut nodes)
        }
        _ => Err(format!("unknown action: {action}")),
    }
}

/// Maps each node to `active`, `inactive`, or `failed`.
pub fn status() -> BTreeMap<String, String> {
    let mut nodes = match NODES.lock() {
        Ok(nodes) => nodes,
        Err(_) => return UNITS.iter().map(|u| (u.to_string(), "unknown".to_string())).collect(),
    };
    UNITS
        .iter()
        .zip(nodes.iter_mut())
        .map(|(&unit, slot)| {
            let state = match slot.as_mut().map(Child::try_wait) {
                None => "inactive",
                Some(Ok(None)) => "active",
                Some(_) => "failed",
            };
            (unit.to_string(), state.to_string())
        })
        .collect()
}

fn node_binary() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|err| err.to_string())?;
    let path = exe.parent().map(|dir| dir.join(NODE_BINARY)).unwrap_or_else(|| NODE_BINARY.into());
    if path.is_file() {
        Ok(path)
    } else {
        Err(format!("{} not found next to the application", path.display()))
    }
}

fn start_all(nodes: &mut [Option<Child>; 3]) -> Result<(), String> {
    let binary = node_binary()?;
    let work_dir = platform::app_data_dir();
    std::fs::create_dir_all(work_dir.join("data")).map_err(|err| err.to_string())?;
    for (slot, (port, bootstrap)) in nodes.iter_mut().zip(NODE_ARGS) {
        let running = matches!(slot.as_mut().map(Child::try_wait), Some(Ok(None)));
        if running {
            continue;
        }
        let mut command = Command::new(&binary);
        command.arg(port).current_dir(&work_dir).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        if let Some(addr) = bootstrap {
            command.arg(addr);
        }
        let child = platform::hide_window(&mut command)
            .spawn()
            .map_err(|err| format!("could not start node on port {port}: {err}"))?;
        *slot = Some(child);
    }
    Ok(())
}

fn stop_all(nodes: &mut [Option<Child>; 3]) {
    for slot in nodes.iter_mut() {
        if let Some(mut child) = slot.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
