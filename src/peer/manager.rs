use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use crate::peer::db::PeerStore;

pub struct PeerManager {
    connected_peers: Arc<Mutex<HashSet<String>>>,
    store: Option<PeerStore>,
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            connected_peers: Arc::new(Mutex::new(HashSet::new())),
            store: None,
        }
    }

    pub fn with_store(db_path: &str) -> Self {
        let store = PeerStore::new(db_path).ok();
        let mut initial_peers = HashSet::new();
        
        if let Some(ref s) = store {
            if let Ok(saved_peers) = s.load_peers() {
                for peer in saved_peers {
                    initial_peers.insert(peer);
                }
            }
        }

        Self {
            connected_peers: Arc::new(Mutex::new(initial_peers)),
            store,
        }
    }

    pub fn add_peer(&self, peer_addr: String) {
        let mut peers = self.connected_peers.lock().unwrap();
        if peers.insert(peer_addr.clone()) {
            if let Some(ref s) = self.store {
                let _ = s.save_peer(&peer_addr);
            }
        }
    }

    pub fn remove_peer(&self, peer_addr: &str) {
        let mut peers = self.connected_peers.lock().unwrap();
        if peers.remove(peer_addr) {
            if let Some(ref s) = self.store {
                let _ = s.remove_peer(peer_addr);
            }
        }
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
