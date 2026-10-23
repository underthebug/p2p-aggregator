use ed25519_dalek::{VerifyingKey, Signature, Signer, SigningKey, Verifier};
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};
use anyhow::{Result, anyhow};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AgentPayload {
    pub sender_id: String,
    pub timestamp: u64,
    pub action: String,
    pub nonce: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SignedPayload {
    pub payload: AgentPayload,
    pub signature: String, // Hex-encoded ed25519 signature
    pub public_key: String, // Hex-encoded ed25519 public key
}

impl SignedPayload {
    pub fn compute_hash(&self) -> Result<String> {
        let serialized = serde_json::to_string(&self.payload)?;
        let mut hasher = Sha256::new();
        hasher.update(serialized.as_bytes());
        Ok(hex::encode(hasher.finalize()))
    }

    pub fn verify_signature(&self) -> Result<bool> {
        let pub_key_bytes = hex::decode(&self.public_key)?;
        let sig_bytes = hex::decode(&self.signature)?;

        if pub_key_bytes.len() != 32 {
            return Err(anyhow!("Invalid public key length"));
        }
        if sig_bytes.len() != 64 {
            return Err(anyhow!("Invalid signature length"));
        }

        let pub_key_arr: [u8; 32] = pub_key_bytes.try_into().map_err(|_| anyhow!("Bad public key conversion"))?;
        let sig_arr: [u8; 64] = sig_bytes.try_into().map_err(|_| anyhow!("Bad signature conversion"))?;

        let verifying_key = VerifyingKey::from_bytes(&pub_key_arr)?;
        let signature = Signature::from_bytes(&sig_arr);

        let serialized_payload = serde_json::to_string(&self.payload)?;
        
        match verifying_key.verify(serialized_payload.as_bytes(), &signature) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}
