use ed25519_dalek::{SigningKey, VerifyingKey, Signer, Verifier, Signature};
use rand::rngs::OsRng;

pub struct CryptoManager;

#[derive(Clone)]
pub struct NodeIdentity {
    pub signing_key: SigningKey,
    pub verifying_key: VerifyingKey,
}

impl NodeIdentity {
    pub fn load_or_generate(_path: &str) -> Self {
        let (signing_key, verifying_key) = CryptoManager::generate_keypair();
        Self { signing_key, verifying_key }
    }

    pub fn sign_challenge(&self, challenge: &[u8]) -> Signature {
        self.signing_key.sign(challenge)
    }

    pub fn verify_challenge(&self, peer_pubkey_bytes: &[u8], challenge: &[u8], sig_bytes: &[u8]) -> bool {
        if let (Ok(pubkey_array), Ok(sig_array)) = (<[u8; 32]>::try_from(peer_pubkey_bytes), <[u8; 64]>::try_from(sig_bytes)) {
            if let Ok(verifying_key) = VerifyingKey::from_bytes(&pubkey_array) {
                let signature = Signature::from_bytes(&sig_array);
                return verifying_key.verify(challenge, &signature).is_ok();
            }
        }
        false
    }
}

impl CryptoManager {
    pub fn generate_keypair() -> (SigningKey, VerifyingKey) {
        let signing_key = SigningKey::generate(&mut OsRng);
        let verifying_key = signing_key.verifying_key();
        (signing_key, verifying_key)
    }
}
