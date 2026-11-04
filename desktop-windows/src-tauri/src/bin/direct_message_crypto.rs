use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use chacha20poly1305::{
    aead::{Aead, Payload},
    ChaCha20Poly1305, KeyInit, Nonce,
};
use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{Signature, VerifyingKey};
use hkdf::Hkdf;
use p2p_desktop_lib::db::DbManager;
use rand::{rngs::OsRng, RngCore};
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

pub const ENCRYPTION_VERSION: &str = "x25519-chacha20poly1305-v1";
const KEY_DERIVATION_SALT: &[u8] = b"p2p-aggregator/direct-message/v1";
const NONCE_BYTES: usize = 12;

#[derive(Debug, Clone)]
pub struct EncryptedPayload {
    pub ephemeral_public_key_hex: String,
    pub nonce_hex: String,
    pub ciphertext: String,
    pub signature_hex: String,
}

#[derive(Debug)]
pub struct DecryptedPayload {
    pub content: String,
    pub reply_to_broadcast: Option<String>,
}

pub fn encrypt(
    sender: &DbManager,
    message_id: &str,
    sender_id: &str,
    recipient_id: &str,
    timestamp: i64,
    reply_to_broadcast: Option<&str>,
    content: &str,
) -> Result<EncryptedPayload, String> {
    if sender.local_sender_id() != sender_id {
        return Err("Sender ID does not match the local identity key.".to_string());
    }
    let recipient_public_key = encryption_public_key(recipient_id)?;
    let ephemeral_secret = StaticSecret::random_from_rng(OsRng);
    let ephemeral_public_key = PublicKey::from(&ephemeral_secret);
    let ephemeral_public_key_hex = hex::encode(ephemeral_public_key.as_bytes());
    let mut nonce = [0u8; NONCE_BYTES];
    OsRng.fill_bytes(&mut nonce);
    let nonce_hex = hex::encode(nonce);
    let associated_data = associated_data(
        message_id,
        sender_id,
        recipient_id,
        timestamp,
        &ephemeral_public_key_hex,
        &nonce_hex,
    )?;
    let shared_secret = ephemeral_secret.diffie_hellman(&recipient_public_key);
    let key = derive_key(shared_secret.as_bytes(), &associated_data)?;
    let cipher = ChaCha20Poly1305::new_from_slice(&key)
        .map_err(|_| "Could not initialize direct-message encryption.".to_string())?;
    let plaintext = encode_payload(content, reply_to_broadcast)?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &associated_data,
            },
        )
        .map_err(|_| "Could not encrypt direct message.".to_string())?;
    let ciphertext = STANDARD_NO_PAD.encode(ciphertext);
    let signing_bytes = signature_bytes(
        message_id,
        sender_id,
        recipient_id,
        timestamp,
        &ephemeral_public_key_hex,
        &nonce_hex,
        &ciphertext,
    )?;
    let signature_hex = hex::encode(sender.sign_message(&signing_bytes));

    Ok(EncryptedPayload {
        ephemeral_public_key_hex,
        nonce_hex,
        ciphertext,
        signature_hex,
    })
}

pub fn decrypt(
    recipient: &DbManager,
    message_id: &str,
    sender_id: &str,
    recipient_id: &str,
    timestamp: i64,
    ephemeral_public_key_hex: &str,
    nonce_hex: &str,
    ciphertext: &str,
    signature_hex: &str,
) -> Result<DecryptedPayload, String> {
    if recipient.local_sender_id() != recipient_id {
        return Err("Recipient ID does not match the local identity key.".to_string());
    }
    let verifying_key = signing_public_key(sender_id)?;
    let signature_bytes = signature_bytes(
        message_id,
        sender_id,
        recipient_id,
        timestamp,
        ephemeral_public_key_hex,
        nonce_hex,
        ciphertext,
    )?;
    let signature = hex::decode(signature_hex)
        .map_err(|_| "Invalid direct-message signature encoding.".to_string())?;
    let signature = Signature::from_slice(&signature)
        .map_err(|_| "Invalid direct-message signature length.".to_string())?;
    verifying_key
        .verify_strict(&signature_bytes, &signature)
        .map_err(|_| "Direct-message sender signature is invalid.".to_string())?;

    let ephemeral_public_key = decode_fixed::<32>(ephemeral_public_key_hex)?;
    let nonce = decode_fixed::<NONCE_BYTES>(nonce_hex)?;
    let ciphertext = STANDARD_NO_PAD
        .decode(ciphertext)
        .map_err(|_| "Invalid direct-message ciphertext encoding.".to_string())?;
    let ephemeral_public_key = PublicKey::from(ephemeral_public_key);
    let recipient_secret = StaticSecret::from(recipient.encryption_secret());
    let shared_secret = recipient_secret.diffie_hellman(&ephemeral_public_key);
    let associated_data = associated_data(
        message_id,
        sender_id,
        recipient_id,
        timestamp,
        ephemeral_public_key_hex,
        nonce_hex,
    )?;
    let key = derive_key(shared_secret.as_bytes(), &associated_data)?;
    let cipher = ChaCha20Poly1305::new_from_slice(&key)
        .map_err(|_| "Could not initialize direct-message decryption.".to_string())?;
    let plaintext = cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &ciphertext,
                aad: &associated_data,
            },
        )
        .map_err(|_| "Direct-message authentication failed.".to_string())?;
    decode_payload(&plaintext)
}

fn encode_payload(content: &str, reply_to_broadcast: Option<&str>) -> Result<Vec<u8>, String> {
    let content_len = u32::try_from(content.len())
        .map_err(|_| "Direct-message content is too large.".to_string())?;
    let reply_len = reply_to_broadcast
        .map(str::len)
        .map(u32::try_from)
        .transpose()
        .map_err(|_| "Broadcast reference is too large.".to_string())?;
    let mut payload =
        Vec::with_capacity(9 + content.len() + reply_to_broadcast.map_or(0, str::len));
    payload.extend_from_slice(&content_len.to_be_bytes());
    payload.extend_from_slice(content.as_bytes());
    match (reply_to_broadcast, reply_len) {
        (Some(reply), Some(length)) => {
            payload.push(1);
            payload.extend_from_slice(&length.to_be_bytes());
            payload.extend_from_slice(reply.as_bytes());
        }
        (None, None) => payload.push(0),
        _ => return Err("Direct-message payload is invalid.".to_string()),
    }
    Ok(payload)
}

fn decode_payload(payload: &[u8]) -> Result<DecryptedPayload, String> {
    let mut cursor = 0;
    let content_len = read_payload_length(payload, &mut cursor)?;
    let content_bytes = read_payload_bytes(payload, &mut cursor, content_len)?;
    let content = String::from_utf8(content_bytes.to_vec())
        .map_err(|_| "Decrypted direct-message content is not UTF-8.".to_string())?;
    let reply_to_broadcast = match payload.get(cursor) {
        Some(0) if cursor + 1 == payload.len() => None,
        Some(1) => {
            cursor += 1;
            let reply_len = read_payload_length(payload, &mut cursor)?;
            let reply_bytes = read_payload_bytes(payload, &mut cursor, reply_len)?;
            if cursor != payload.len() {
                return Err("Decrypted direct-message payload has trailing data.".to_string());
            }
            Some(
                String::from_utf8(reply_bytes.to_vec())
                    .map_err(|_| "Decrypted broadcast reference is not UTF-8.".to_string())?,
            )
        }
        _ => return Err("Decrypted direct-message payload is invalid.".to_string()),
    };
    Ok(DecryptedPayload {
        content,
        reply_to_broadcast,
    })
}

fn read_payload_length(payload: &[u8], cursor: &mut usize) -> Result<usize, String> {
    let length_end = cursor
        .checked_add(4)
        .ok_or_else(|| "Decrypted direct-message payload is invalid.".to_string())?;
    let length_bytes = payload
        .get(*cursor..length_end)
        .ok_or_else(|| "Decrypted direct-message payload is truncated.".to_string())?;
    *cursor = length_end;
    let length = u32::from_be_bytes(
        length_bytes
            .try_into()
            .map_err(|_| "Decrypted direct-message payload is invalid.".to_string())?,
    );
    usize::try_from(length)
        .map_err(|_| "Decrypted direct-message payload is too large.".to_string())
}

fn read_payload_bytes<'a>(
    payload: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], String> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| "Decrypted direct-message payload is invalid.".to_string())?;
    let bytes = payload
        .get(*cursor..end)
        .ok_or_else(|| "Decrypted direct-message payload is truncated.".to_string())?;
    *cursor = end;
    Ok(bytes)
}

fn encryption_public_key(peer_id: &str) -> Result<PublicKey, String> {
    let public_bytes = peer_signing_key_bytes(peer_id)?;
    VerifyingKey::from_bytes(&public_bytes)
        .map_err(|_| "Recipient peer ID does not contain a valid signing key.".to_string())?;
    let point = CompressedEdwardsY(public_bytes)
        .decompress()
        .ok_or_else(|| "Recipient peer ID does not contain a valid encryption key.".to_string())?;
    Ok(PublicKey::from(point.to_montgomery().to_bytes()))
}

fn signing_public_key(peer_id: &str) -> Result<VerifyingKey, String> {
    let public_bytes = peer_signing_key_bytes(peer_id)?;
    VerifyingKey::from_bytes(&public_bytes)
        .map_err(|_| "Sender peer ID does not contain a valid signing key.".to_string())
}

fn peer_signing_key_bytes(peer_id: &str) -> Result<[u8; 32], String> {
    let encoded = peer_id
        .strip_prefix("did:ed25519:")
        .ok_or_else(|| "Peer ID is not a supported Ed25519 identity.".to_string())?;
    decode_fixed::<32>(encoded)
}

fn decode_fixed<const N: usize>(encoded: &str) -> Result<[u8; N], String> {
    let bytes =
        hex::decode(encoded).map_err(|_| "Invalid direct-message key encoding.".to_string())?;
    bytes
        .try_into()
        .map_err(|_| "Invalid direct-message key length.".to_string())
}

fn derive_key(shared_secret: &[u8; 32], context: &[u8]) -> Result<[u8; 32], String> {
    if shared_secret.iter().all(|byte| *byte == 0) {
        return Err("Direct-message key agreement failed.".to_string());
    }
    let hkdf = Hkdf::<Sha256>::new(Some(KEY_DERIVATION_SALT), shared_secret);
    let mut key = [0u8; 32];
    hkdf.expand(context, &mut key)
        .map_err(|_| "Could not derive direct-message key.".to_string())?;
    Ok(key)
}

fn associated_data(
    message_id: &str,
    sender_id: &str,
    recipient_id: &str,
    timestamp: i64,
    ephemeral_public_key_hex: &str,
    nonce_hex: &str,
) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&(
        ENCRYPTION_VERSION,
        message_id,
        sender_id,
        recipient_id,
        timestamp,
        ephemeral_public_key_hex,
        nonce_hex,
    ))
    .map_err(|err| err.to_string())
}

fn signature_bytes(
    message_id: &str,
    sender_id: &str,
    recipient_id: &str,
    timestamp: i64,
    ephemeral_public_key_hex: &str,
    nonce_hex: &str,
    ciphertext: &str,
) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&(
        ENCRYPTION_VERSION,
        message_id,
        sender_id,
        recipient_id,
        timestamp,
        ephemeral_public_key_hex,
        nonce_hex,
        ciphertext,
    ))
    .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::{decrypt, encrypt, encryption_public_key, ENCRYPTION_VERSION};
    use p2p_desktop_lib::db::DbManager;
    use x25519_dalek::{PublicKey, StaticSecret};

    fn identity() -> DbManager {
        DbManager::new(":memory:").unwrap()
    }

    #[test]
    fn encryption_key_is_bound_to_ed25519_peer_identity() {
        let db = identity();
        let private_key = StaticSecret::from(db.encryption_secret());
        let public_key = PublicKey::from(&private_key);

        assert_eq!(
            public_key.as_bytes(),
            encryption_public_key(&db.local_sender_id())
                .unwrap()
                .as_bytes()
        );
    }

    #[test]
    fn recipient_can_decrypt_but_wrong_identity_cannot() {
        let sender = identity();
        let recipient = identity();
        let wrong_recipient = identity();
        let payload = encrypt(
            &sender,
            "message-1",
            &sender.local_sender_id(),
            &recipient.local_sender_id(),
            123,
            Some("broadcast-7"),
            "private text\n\\",
        )
        .unwrap();

        assert!(!payload.ciphertext.contains("private text"));
        let decrypted = decrypt(
            &recipient,
            "message-1",
            &sender.local_sender_id(),
            &recipient.local_sender_id(),
            123,
            &payload.ephemeral_public_key_hex,
            &payload.nonce_hex,
            &payload.ciphertext,
            &payload.signature_hex,
        )
        .unwrap();
        assert_eq!(decrypted.content, "private text\n\\");
        assert_eq!(decrypted.reply_to_broadcast.as_deref(), Some("broadcast-7"));

        assert!(decrypt(
            &wrong_recipient,
            "message-1",
            &sender.local_sender_id(),
            &recipient.local_sender_id(),
            123,
            &payload.ephemeral_public_key_hex,
            &payload.nonce_hex,
            &payload.ciphertext,
            &payload.signature_hex,
        )
        .is_err());
    }

    #[test]
    fn ciphertext_tampering_is_rejected() {
        let sender = identity();
        let recipient = identity();
        let mut payload = encrypt(
            &sender,
            "message-2",
            &sender.local_sender_id(),
            &recipient.local_sender_id(),
            456,
            None,
            "authenticated",
        )
        .unwrap();
        payload.ciphertext.push('A');

        assert!(decrypt(
            &recipient,
            "message-2",
            &sender.local_sender_id(),
            &recipient.local_sender_id(),
            456,
            &payload.ephemeral_public_key_hex,
            &payload.nonce_hex,
            &payload.ciphertext,
            &payload.signature_hex,
        )
        .is_err());
    }

    #[test]
    fn unsupported_peer_identity_is_rejected() {
        let sender = identity();
        assert!(encrypt(
            &sender,
            "message-3",
            &sender.local_sender_id(),
            "peer-without-a-key",
            789,
            None,
            "refuse plaintext",
        )
        .is_err());
        assert_eq!(ENCRYPTION_VERSION, "x25519-chacha20poly1305-v1");
    }
}
