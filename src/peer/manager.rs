use std::sync::{Arc, Mutex};
use std::collections::HashSet;

pub struct PeerManager {
    connected_peers: Arc<Mutex<HashSet<String>>>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            connected_peers: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn add_peer(&self, peer_addr: String) {
        let mut peers = self.connected_peers.lock().unwrap();
        peers.insert(peer_addr);
    }

    pub fn get_peer_count(&self) -> usize {
        let peers = self.connected_peers.lock().unwrap();
        peers.len()
    }

    pub fn get_peers(&self) -> Vec<String> {
        let peers = self.connected_peers.lock().unwrap();
        peers.iter().cloned().collect()
    }
}
