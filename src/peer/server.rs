use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::peer::manager::PeerManager;
use crate::crypto::NodeIdentity;
use crate::consensus::{ConsensusEngine, RaftMessage};
use rand::rngs::OsRng;
use rand::RngCore;

pub async fn start_p2p_listener(
    bind_addr: &str, 
    peer_manager: Arc<PeerManager>,
    consensus_engine: Arc<Mutex<ConsensusEngine>>
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    println!("P2P TCP encrypted listener active on {}", bind_addr);

    tokio::spawn(async move {
        let identity = Arc::new(NodeIdentity::load_or_generate("node.key"));

        loop {
            match listener.accept().await {
                Ok((mut socket, peer_addr)) => {
                    let pm = Arc::clone(&peer_manager);
                    let id = Arc::clone(&identity);
                    let consensus = Arc::clone(&consensus_engine);
                    
                    tokio::spawn(async move {
                        let mut remote_pubkey = [0u8; 32];
                        if socket.read_exact(&mut remote_pubkey).await.is_err() {
                            return;
                        }

                        let pub_key_bytes = id.verifying_key.to_bytes();
                        if socket.write_all(&pub_key_bytes).await.is_err() {
                            return;
                        }

                        let mut challenge = [0u8; 32];
                        OsRng.fill_bytes(&mut challenge);
                        if socket.write_all(&challenge).await.is_err() {
                            return;
                        }

                        let mut sig_bytes = [0u8; 64];
                        if socket.read_exact(&mut sig_bytes).await.is_err() {
                            return;
                        }

                        let authenticated = id.verify_challenge(&remote_pubkey, &challenge, &sig_bytes);
                        
                        if authenticated {
                            if socket.write_all(b"OK").await.is_err() {
                                return;
                            }
                            pm.add_peer(peer_addr.to_string());
                            println!("[CryptoMesh] Successfully authenticated secure peer: {}", peer_addr);

                            loop {
                                let mut len_buf = [0u8; 4];
                                if socket.read_exact(&mut len_buf).await.is_err() {
                                    break;
                                }
                                let msg_len = u32::from_be_bytes(len_buf) as usize;
                                let mut msg_buf = vec![0u8; msg_len];
                                if socket.read_exact(&mut msg_buf).await.is_err() {
                                    break;
                                }

                                if let Ok(raft_msg) = bincode::deserialize::<RaftMessage>(&msg_buf) {
                                    let response = match raft_msg {
                                        RaftMessage::AppendEntries { term, leader_id, entries, .. } => {
                                            let mut engine = consensus.lock().await;
                                            engine.handle_append_entries(term, leader_id, entries).await
                                        }
                                        RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                                            let mut engine = consensus.lock().await;
                                            engine.handle_request_vote(term, candidate_id, last_log_index, last_log_term).await
                                        }
                                        _ => {
                                            RaftMessage::AppendEntriesResponse { term: 0, success: false, match_index: 0 }
                                        }
                                    };

                                    if let Ok(resp_bytes) = bincode::serialize(&response) {
                                        let resp_len = (resp_bytes.len() as u32).to_be_bytes();
                                        if socket.write_all(&resp_len).await.is_err() {
                                            break;
                                        }
                                        if socket.write_all(&resp_bytes).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                        } else {
                            let _ = socket.write_all(b"ER").await;
                            println!("[CryptoMesh] Authentication FAILED for peer: {}", peer_addr);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("P2P accept error: {}", e);
                }
            }
        }
    });

    Ok(())
}
