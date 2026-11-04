const invoke = (command, args) => {
    if (window.__TAURI__?.core?.invoke) return window.__TAURI__.core.invoke(command, args);
    if (window.__TAURI_INTERNALS__?.invoke) return window.__TAURI_INTERNALS__.invoke(command, args);
    return Promise.reject(new Error('Tauri IPC is unavailable.'));
};

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
    if (!listContainer) return;
    try {
        const streams = await invoke('get_user_feeds');
        listContainer.replaceChildren();
        if (streams.length === 0) {
            const empty = document.createElement('span');
            empty.textContent = 'No custom streams added yet.';
            listContainer.appendChild(empty);
            return;
        }
        streams.forEach(stream => {
            const item = document.createElement('div');
            item.style.cssText = 'background:#1a1a1a;border:1px solid #333;padding:8px;display:flex;justify-content:space-between;align-items:center;';
            const details = document.createElement('div');
            details.style.cssText = 'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:260px;';
            const name = document.createElement('strong');
            name.style.color = '#00ffcc';
            name.textContent = stream.name;
            const url = document.createElement('small');
            url.style.color = '#888';
            url.textContent = stream.url;
            details.append(name, document.createElement('br'), url);

            const actions = document.createElement('div');
            actions.style.cssText = 'display:flex;gap:5px;';
            const edit = document.createElement('button');
            edit.textContent = 'Edit';
            edit.onclick = () => window.editStream(stream.id, stream.name, stream.url);
            const remove = document.createElement('button');
            remove.textContent = 'Del';
            remove.onclick = () => window.deleteStream(stream.id);
            actions.append(edit, remove);
            item.append(details, actions);
            listContainer.appendChild(item);
        });
    } catch (err) {
        console.error('Failed to load custom streams:', err);
    }
}

if (form) {
    form.onsubmit = async event => {
        event.preventDefault();
        const name = document.getElementById('stream-title').value;
        const url = document.getElementById('stream-url').value;
        try {
            await invoke('add_user_feed', { name, url });
            form.reset();
            document.getElementById('stream-id').value = '';
            await loadStreams();
        } catch (err) {
            alert(`Error saving stream: ${err}`);
        }
    };
}

window.deleteStream = async id => {
    if (confirm('Remove this custom stream?')) {
        await invoke('remove_user_feed', { id });
        await loadStreams();
    }
};

window.editStream = (id, name, url) => {
    document.getElementById('stream-id').value = id;
    document.getElementById('stream-title').value = name;
    document.getElementById('stream-url').value = url;
    if (cancelBtn) cancelBtn.style.display = 'inline-block';
};

if (cancelBtn) {
    cancelBtn.onclick = () => {
        form?.reset();
        document.getElementById('stream-id').value = '';
        cancelBtn.style.display = 'none';
    };
}