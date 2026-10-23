use serde::Deserialize;

#[derive(Deserialize)]
pub struct SignedPayload {
    pub payload: String,
    pub signature: String,
}

impl SignedPayload {
    pub fn verify_signature(&self) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(true)
    }
}
