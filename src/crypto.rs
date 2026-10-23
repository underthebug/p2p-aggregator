use ed25519_dalek::{SigningKey, VerifyingKey, Signer, Signature, Verifier};
use rand::rngs::OsRng;
use rand::RngCore;
use std::fs;
use std::path::Path;

pub struct NodeIdentity {
    pub signing_key: SigningKey,
    pub verifying_key: VerifyingKey,
}

impl NodeIdentity {
    pub fn load_or_generate<P: AsRef<Path>>(key_path: P) -> Self {
        let path = key_path.as_ref();
        let signing_key = if path.exists() {
            if let Ok(bytes) = fs::read(path) {
                if bytes.len() == 32 {
                    let mut key_bytes = [0u8; 32];
                    key_bytes.copy_from_slice(&bytes);
                    SigningKey::from_bytes(&key_bytes)
                } else {
                    let mut key_bytes = [0u8; 32];
                    OsRng.fill_bytes(&mut key_bytes);
                    SigningKey::from_bytes(&key_bytes)
                }
            } else {
                let mut key_bytes = [0u8; 32];
                OsRng.fill_bytes(&mut key_bytes);
                SigningKey::from_bytes(&key_bytes)
            }
        } else {
            let mut key_bytes = [0u8; 32];
            OsRng.fill_bytes(&mut key_bytes);
            let key = SigningKey::from_bytes(&key_bytes);
            if let Some(parent) = path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::write(path, key.to_bytes());
            key
        };

        let verifying_key = VerifyingKey::from(&signing_key);
        Self { signing_key, verifying_key }
    }

    pub fn sign_challenge(&self, challenge: &[u8]) -> Signature {
        self.signing_key.sign(challenge)
    }

    pub fn verify_challenge(&self, peer_pubkey_bytes: &[u8], challenge: &[u8], signature_bytes: &[u8]) -> bool {
        if let Ok(verifying_key) = VerifyingKey::from_bytes(peer_pubkey_bytes.try_into().unwrap_or(&[0; 32])) {
            if let Ok(signature) = Signature::try_from(signature_bytes) {
                return verifying_key.verify(challenge, &signature).is_ok();
            }
        }
        false
    }
}
