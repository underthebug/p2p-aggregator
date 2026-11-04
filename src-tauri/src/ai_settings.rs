//! Address of the Ollama server (this computer or another machine on the LAN).

use std::path::Path;

pub const SETTINGS_PATH: &str = "data/ai_settings.json";
pub const DEFAULT_URL: &str = "http://127.0.0.1:11434";

/// Accepts an http(s) base URL, adds a missing scheme or port, and drops any path.
pub fn normalize_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(DEFAULT_URL.to_string());
    }
    let with_scheme = if trimmed.contains("://") { trimmed.to_string() } else { format!("http://{trimmed}") };
    let url = reqwest::Url::parse(&with_scheme).map_err(|_| "Address is not a valid URL".to_string())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("Address must start with http:// or https://".to_string());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("Credentials in the address are not supported".to_string());
    }
    let host = url.host_str().ok_or_else(|| "Address must include a host".to_string())?;
    let port = url.port().unwrap_or(11434);
    let host = if host.contains(':') { format!("[{host}]") } else { host.to_string() };
    Ok(format!("{}://{}:{}", url.scheme(), host, port))
}

pub fn load(path: &Path) -> String {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .and_then(|json| json.get("ollama_url")?.as_str().map(str::to_string))
        .and_then(|raw| normalize_url(&raw).ok())
        .unwrap_or_else(|| DEFAULT_URL.to_string())
}

pub fn save(path: &Path, raw: &str) -> Result<String, String> {
    let url = normalize_url(raw)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let body = serde_json::json!({ "ollama_url": url });
    std::fs::write(path, body.to_string()).map_err(|e| format!("Failed to save AI settings: {e}"))?;
    Ok(url)
}

/// One non-streaming chat call to the configured Ollama server.
pub async fn chat(model: &str, messages: serde_json::Value, timeout_secs: u64) -> Result<String, String> {
    let base = load(Path::new(SETTINGS_PATH));
    let response = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .build()
        .map_err(|error| error.to_string())?
        .post(format!("{base}/api/chat"))
        .json(&serde_json::json!({
            "model": model,
            "messages": messages,
            "stream": false,
            "keep_alive": "30m",
            "options": { "num_predict": 512 }
        }))
        .send()
        .await
        .map_err(|error| format!("Local Ollama request failed: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Local Ollama returned an error: {error}"))?;
    let response: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;
    response
        .pointer("/message/content")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "Local Ollama returned no response text".to_string())
}

/// Picks a preferred installed model, else the first one the server lists.
pub async fn pick_model() -> Result<String, String> {
    let base = load(Path::new(SETTINGS_PATH));
    let tags: serde_json::Value = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .map_err(|error| error.to_string())?
        .get(format!("{base}/api/tags"))
        .send()
        .await
        .map_err(|error| format!("Could not reach Ollama at {base}: {error}"))?
        .json()
        .await
        .map_err(|error| error.to_string())?;
    let names: Vec<&str> = tags
        .get("models")
        .and_then(serde_json::Value::as_array)
        .map(|models| models.iter().filter_map(|m| m.get("name")?.as_str()).collect())
        .unwrap_or_default();
    ["llama3.1:8b", "gemma3:4b", "llama3.2:1b"]
        .iter()
        .find(|preferred| names.contains(preferred))
        .copied()
        .or_else(|| names.first().copied())
        .map(str::to_string)
        .ok_or_else(|| "The AI server has no models installed".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_lan_addresses() {
        assert_eq!(normalize_url("").unwrap(), DEFAULT_URL);
        assert_eq!(normalize_url("192.168.1.50").unwrap(), "http://192.168.1.50:11434");
        assert_eq!(normalize_url(" http://gpu-box.local:8080/api ").unwrap(), "http://gpu-box.local:8080");
        assert!(normalize_url("ftp://host").is_err());
        assert!(normalize_url("http://user:pw@host").is_err());
    }

    #[test]
    fn save_then_load_round_trips() {
        let path = std::env::temp_dir().join(format!("ai_settings_test_{}.json", std::process::id()));
        assert_eq!(save(&path, "10.0.0.7:11434").unwrap(), "http://10.0.0.7:11434");
        assert_eq!(load(&path), "http://10.0.0.7:11434");
        let _ = std::fs::remove_file(&path);
        assert_eq!(load(&path), DEFAULT_URL);
    }
}
