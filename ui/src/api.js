// Hybrid API client: uses local REST API if served via web, falls back to Tauri IPC
const getApiBase = () => {
  return window.location.protocol.startsWith('http') ? '' : 'http://192.168.1.87:41872';
};

export async function getMeshBroadcasts() {
  try {
    const res = await fetch(`${getApiBase()}/api/broadcasts`);
    if (res.ok) return await res.json();
  } catch (err) {
    console.warn("REST API unreachable, trying Tauri IPC...", err);
  }
  
  if (window.__TAURI__?.core?.invoke) {
    return await window.__TAURI__.core.invoke('fetch_mesh_broadcasts');
  }
  return [];
}

export async function postMeshBroadcast(payload) {
  try {
    const res = await fetch(`${getApiBase()}/api/broadcasts`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    if (res.ok) return true;
  } catch (err) {
    console.warn("REST POST failed:", err);
  }
  return false;
}

export async function askAiNode(prompt, model = "qwen2.5:7b") {
  try {
    const res = await fetch('/api/ai/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt, model })
    });
    if (res.ok) {
      const data = await res.json();
      return data.reply || data.response || "No response generated.";
    }
  } catch (err) {
    console.warn("AI Node request failed:", err);
  }
  return "AI Node offline or unreachable.";
}
