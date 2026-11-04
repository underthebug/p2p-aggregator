use ed25519_dalek::{SigningKey, VerifyingKey, Signer, Verifier, Signature};
use rand::rngs::OsRng;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

pub struct CryptoManager;

#[derive(Clone)]
pub struct NodeIdentity {
    pub signing_key: SigningKey,
    pub verifying_key: VerifyingKey,
}

impl NodeIdentity {
    pub fn load_or_generate(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let signing_key = match fs::read(path) {
            Ok(bytes) => {
                let seed: [u8; 32] = bytes.try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "identity key must be exactly 32 bytes")
                })?;
                SigningKey::from_bytes(&seed)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
                    fs::create_dir_all(parent)?;
                }
                let (generated, _) = CryptoManager::generate_keypair();
                let mut options = OpenOptions::new();
                options.write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;
                    options.mode(0o600);
                }
                match options.open(path) {
                    Ok(mut file) => {
                        file.write_all(&generated.to_bytes())?;
                        generated
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        let bytes = fs::read(path)?;
                        let seed: [u8; 32] = bytes.try_into().map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "identity key must be exactly 32 bytes")
                        })?;
                        SigningKey::from_bytes(&seed)
                    }
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        let verifying_key = signing_key.verifying_key();
        Ok(Self { signing_key, verifying_key })
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

#[cfg(test)]
mod tests {
    use super::NodeIdentity;

    #[test]
    fn identity_key_is_reused_across_loads() {
        let path = std::env::temp_dir().join(format!("p2p-identity-{}.key", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let first = NodeIdentity::load_or_generate(&path).unwrap();
        let second = NodeIdentity::load_or_generate(&path).unwrap();
        assert_eq!(first.verifying_key.to_bytes(), second.verifying_key.to_bytes());

        let _ = std::fs::remove_file(path);
    }
}
