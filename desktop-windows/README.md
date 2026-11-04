# Windows Desktop App

This directory contains the Windows Tauri desktop app, its static UI, and the
Rust node binaries it runs.

## Build on Windows

Install the stable Rust MSVC toolchain, Visual Studio C++ Build Tools with the
Windows SDK, and the WebView2 Runtime. Install the Tauri CLI to build the
desktop app; install NSIS as well if you want to produce the configured
installer.

From `desktop-windows`, build the node and daemon binaries and stage them for
Tauri:

```powershell
Push-Location core
cargo build --release --bin p2p-aggregator
Pop-Location

Push-Location src-tauri
cargo build --release --bin p2p-daemon
Pop-Location

New-Item -ItemType Directory -Force stage | Out-Null
Copy-Item core\target\release\p2p-aggregator.exe stage\
Copy-Item src-tauri\target\release\p2p-daemon.exe stage\

Push-Location src-tauri
cargo tauri build
Pop-Location
```

The desktop executable is under `src-tauri\target\release`. When NSIS is
installed, Tauri also creates the installer under `src-tauri\target\release\bundle`.

The UI is static HTML and does not require an npm install to build the app.
The optional Tailwind CSS build uses the dependencies declared in `ui\package.json`.

## Direct-message encryption

The mesh HTTP daemon encrypts direct-message content with ephemeral X25519
key agreement and ChaCha20-Poly1305, then signs each encrypted envelope with
the sender's Ed25519 identity. Encryption keys are derived from the existing
persistent identity, so recipients can authenticate messages using the peer ID
shown on broadcasts. Plaintext legacy direct messages are rejected.

Message content and broadcast replies are encrypted; sender/recipient IDs,
message IDs, timestamps, and delivery acknowledgements remain visible to the
network. The daemon keeps plaintext only at the sending and receiving
endpoints. As the recipient encryption key is long-lived, a later compromise
of that identity key could expose previously captured messages. This is not a
replacement for a forward-secret ratcheting protocol.

In the browser UI, use **Copy sender ID** on a feed card, then paste that ID
into the direct-message recipient field. **Message sender** fills the field
directly; replies can queue while the recipient is offline.

For Linux cross-compilation, see `build-windows.sh` for the required `cargo-xwin`
and LLVM toolchain.
