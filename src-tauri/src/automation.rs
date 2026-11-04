//! Switches for behaviour that runs on its own. Everything here is off until the user turns it on.

use serde::{Deserialize, Serialize};
use std::path::Path;

pub const SETTINGS_PATH: &str = "data/automation.json";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Automation {
    /// Start the three node services when the desktop app opens.
    #[serde(default)]
    pub auto_start_nodes: bool,
    /// Let the AI write headlines for cards that match Feed Monitor keywords.
    #[serde(default)]
    pub ai_labeling: bool,
}

pub fn load(path: &Path) -> Automation {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

pub fn save(path: &Path, settings: &Automation) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let json = serde_json::to_string_pretty(settings).map_err(|e| e.to_string())?;
    std::fs::write(path, json).map_err(|e| format!("Failed to save automation settings: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn everything_is_off_by_default_and_round_trips() {
        let path = std::env::temp_dir().join(format!("automation_test_{}.json", std::process::id()));
        assert_eq!(load(&path), Automation::default());
        assert!(!Automation::default().auto_start_nodes && !Automation::default().ai_labeling);

        let on = Automation { auto_start_nodes: true, ai_labeling: false };
        save(&path, &on).unwrap();
        assert_eq!(load(&path), on);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_fields_stay_off() {
        let path = std::env::temp_dir().join(format!("automation_partial_{}.json", std::process::id()));
        std::fs::write(&path, "{\"ai_labeling\": true}").unwrap();
        assert_eq!(load(&path), Automation { auto_start_nodes: false, ai_labeling: true });
        let _ = std::fs::remove_file(&path);
    }
}
