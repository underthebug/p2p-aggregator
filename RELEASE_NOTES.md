# Beta Release Notes

## Added
- On-screen UI size control (plus/minus) in the bottom-right corner, saved across restarts.
- WOT verifier performs real Ed25519 signature verification and SHA-256 hashing.

## Fixed
- Desktop app showed "Unavailable" for the session ID. The UI now finds the daemon on ports 41872–41891, and the daemon allows Tauri origins.
- Journal entries are recorded with a real status (`SENT`) instead of a hardcoded `PENDING`.

## Known Issues
- The desktop daemon's outbox is in-memory. Restarting it drops queued messages. Identical messages are deduplicated only while still queued.
- The root `p2p-aggregator` binary, which has outbox dedupe, is not in the release package.
- The tampered-signature case for WOT verification is not yet tested.

## Install
- Release package: `p2p-release.tar.gz` (laptop directory). Checksums are in `SHA256SUMS`.
- The daemon serves the browser UI only with `--browser-ui` or `MESH_BROWSER_UI=1`.
