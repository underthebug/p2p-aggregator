use p2p_aggregator::wot::verifier::{AgentPayload, SignedPayload};
use ed25519_dalek::{Signer, SigningKey};
use rand::rngs::OsRng;

#[test]
fn test_cryptographic_wot_signature_verification() {
    let mut csprng = OsRng;
    let signing_key = SigningKey::generate(&mut csprng);
    let verifying_key = signing_key.verifying_key();

    let payload = AgentPayload {
        sender_id: "agent_node_alpha".to_string(),
        timestamp: 1711900800,
        action: "execute_macro".to_string(),
        nonce: 42,
    };

    let serialized_payload = serde_json::to_string(&payload).unwrap();
    let signature = signing_key.sign(serialized_payload.as_bytes());

    let signed_payload = SignedPayload {
        payload,
        signature: hex::encode(signature.to_bytes()),
        public_key: hex::encode(verifying_key.to_bytes()),
    };

    let is_valid = signed_payload.verify_signature().unwrap();
    assert!(is_valid, "Valid cryptographic signature failed verification!");

    let payload_hash = signed_payload.compute_hash();
    assert!(payload_hash.is_ok(), "Failed to compute deterministic payload hash.");
    assert_eq!(payload_hash.unwrap().len(), 64, "SHA-256 hash length mismatch.");
}

#[test]
fn test_tampered_payload_fails_verification() {
    let signing_key = SigningKey::generate(&mut OsRng);

    let original = AgentPayload {
        sender_id: "agent_node_alpha".to_string(),
        timestamp: 1711900800,
        action: "execute_macro".to_string(),
        nonce: 42,
    };
    let serialized_payload = serde_json::to_string(&original).unwrap();
    let signature = signing_key.sign(serialized_payload.as_bytes());

    let tampered = AgentPayload {
        sender_id: "agent_node_alpha".to_string(),
        timestamp: 1711900800,
        action: "drop_database".to_string(),
        nonce: 42,
    };

    let signed_payload = SignedPayload {
        payload: tampered,
        signature: hex::encode(signature.to_bytes()),
        public_key: hex::encode(signing_key.verifying_key().to_bytes()),
    };

    let is_valid = signed_payload.verify_signature().unwrap();
    assert!(!is_valid, "Tampered payload passed signature verification!");
}
