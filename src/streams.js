const { invoke } = window.__TAURI__.core;

const modal = document.getElementById('stream-manager-modal');
const openBtn = document.getElementById('open-stream-manager-btn');
const closeBtn = document.getElementById('stream-close-modal-btn');
const form = document.getElementById('stream-form');
const listContainer = document.getElementById('streams-list-container');
const cancelBtn = document.getElementById('stream-cancel-btn');

if (openBtn && closeBtn && modal) {
    openBtn.onclick = () => { modal.style.display = 'flex'; loadStreams(); };
    closeBtn.onclick = () => { modal.style.display = 'none'; };
}

async function loadStreams() {
    try {
        const streams = await invoke('get_user_feeds');
        listContainer.innerHTML = '';
        if (streams.length === 0) {
            listContainer.innerHTML = '<span style="color:#666; font-size:12px;">No custom streams added yet.</span>';
            return;
        }
        streams.forEach(stream => {
            const item = document.createElement('div');
            item.style.cssText = "background:#1a1a1a; border:1px solid #333; padding:8px; display:flex; justify-content:space-between; align-items:center;";
            item.innerHTML = `
                <div style="overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:260px;">
                  <strong style="color:#00ffcc;">${stream.name}</strong><br>
                  <small style="color:#888;">${stream.url}</small>
                </div>
                <div style="display:flex; gap:5px;">
                  <button onclick="editStream(${stream.id}, '${stream.name}', '${stream.url}')" style="background:#333; color:#fff; border:none; padding:3px 6px; cursor:pointer;">Edit</button>
                  <button onclick="deleteStream(${stream.id})" style="background:#522; color:#fff; border:none; padding:3px 6px; cursor:pointer;">Del</button>
                </div>
            `;
            listContainer.appendChild(item);
        });
    } catch (err) {
        console.error("Failed to load custom streams:", err);
    }
}

if (form) {
    form.onsubmit = async (e) => {
        e.preventDefault();
        const name = document.getElementById('stream-title').value;
        const url = document.getElementById('stream-url').value;
        try {
            await invoke('add_user_feed', { name, url });
            form.reset();
            document.getElementById('stream-id').value = '';
            loadStreams();
        } catch (err) {
            alert("Error saving stream: " + err);
        }
    };
}

window.deleteStream = async function(id) {
    if (confirm("Remove this custom stream?")) {
        await invoke('remove_user_feed', { id });
        loadStreams();
    }
};

window.editStream = function(id, name, url) {
    document.getElementById('stream-id').value = id;
    document.getElementById('stream-title').value = name;
    document.getElementById('stream-url').value = url;
    cancelBtn.style.display = 'inline-block';
};

cancelBtn.onclick = () => {
    form.reset();
    document.getElementById('stream-id').value = '';
    cancelBtn.style.display = 'none';
};
