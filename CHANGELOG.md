# Changelog

Versions follow [Semantic Versioning](https://semver.org): MAJOR.MINOR.PATCH.
Keep the version in `Cargo.toml`, `src-tauri/Cargo.toml` and
`src-tauri/tauri.conf.json` the same. The app's Help and About pages read the
version from the running executable.

## 0.2.1

- End-to-end encrypted direct messages in the mesh daemon (X25519,
  ChaCha20-Poly1305, Ed25519-signed envelopes); legacy plaintext is rejected.
  Linux and Windows nodes must both run 0.2.x to message each other.
- Copy sender ID on feed cards and Paste ID / Message sender in the composer.
- Each local node now uses its own BitTorrent DHT port, so three nodes on one
  machine no longer compete for the default port.
- Help and About pages show the real app version.
- Synced with the Windows desktop build (see `windows-v0.2.0`/`0.2.1`).
