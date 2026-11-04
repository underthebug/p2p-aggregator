window.meshApiFetch = async function (input, init = {}) {
    if (window.__TAURI_INTERNALS__) {
        throw new Error('This endpoint requires the separate mesh HTTP daemon and is unavailable in desktop mode.');
    }
    let token = sessionStorage.getItem('mesh_api_token') || '';
    for (let attempt = 0; attempt < 2; attempt += 1) {
        if (!token) {
            token = window.prompt('Enter the local API token from data/api.token:') || '';
            token = token.trim();
            if (!token) throw new Error('A local API token is required.');
            sessionStorage.setItem('mesh_api_token', token);
        }

        const headers = new Headers(init.headers || {});
        headers.set('Authorization', `Bearer ${token}`);
        const response = await fetch(input, { ...init, headers });
        if (response.status !== 401) return response;

        sessionStorage.removeItem('mesh_api_token');
        token = '';
    }
    throw new Error('The local API token was rejected.');
};