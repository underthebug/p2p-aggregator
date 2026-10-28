use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
pub struct AgentPayload {
    pub sender_id: String,
    pub timestamp: u64,
    pub action: String,
    pub nonce: u64,
}

#[derive(Deserialize)]
pub struct SignedPayload {
    pub payload: AgentPayload,
    pub signature: String,
    pub public_key: String,
}

impl SignedPayload {
    fn canonical_bytes(&self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        Ok(serde_json::to_vec(&self.payload)?)
    }

    pub fn verify_signature(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let key_bytes: [u8; 32] = hex::decode(&self.public_key)?
            .try_into()
            .map_err(|_| "public key must be 32 bytes")?;
        let sig_bytes: [u8; 64] = hex::decode(&self.signature)?
            .try_into()
            .map_err(|_| "signature must be 64 bytes")?;
        let key = VerifyingKey::from_bytes(&key_bytes)?;
        let signature = Signature::from_bytes(&sig_bytes);
        Ok(key.verify(&self.canonical_bytes()?, &signature).is_ok())
    }

    pub fn compute_hash(&self) -> Result<String, Box<dyn std::error::Error>> {
        Ok(hex::encode(Sha256::digest(self.canonical_bytes()?)))
    }
}
