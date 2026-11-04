# Changelog

Versions follow [Semantic Versioning](https://semver.org): MAJOR.MINOR.PATCH.
Bump the version in `core/Cargo.toml`, `src-tauri/Cargo.toml` and
`src-tauri/tauri.conf.json` together, then tag the release (`git tag v0.2.0`).
The app's Help and About pages read the version from the running executable.

## 0.2.0

- End-to-end encrypted direct messages in the mesh daemon (X25519,
  ChaCha20-Poly1305, Ed25519-signed envelopes); legacy plaintext is rejected.
- Copy sender ID on feed cards and Paste ID / Message sender in the composer.
- Fixed nodes competing for the DHT port and sharing an incompatible database.
- Build with the npm-installed Tauri CLI.
- Help and About pages show the real app version.

## 0.1.0

- Initial Windows desktop build.
