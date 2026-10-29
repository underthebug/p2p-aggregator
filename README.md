🚀 vX.Y.Z - p2p_aggregator AlphaWe are excited to announce release vX.Y.Z of p2p_aggregator! This release delivers decentralized peer-to-peer networking, local RSS feed ingestion, encrypted messaging, and our lightweight Tauri desktop client.✨ What's New & HighlightsDecentralized P2P Networking: Upgraded libp2p integration for robust peer discovery, encrypted handshakes, and reliable direct node-to-node routing.RSS & Media Ingestion: Enhanced local parsing and synchronization of feeds across active peers.Secure Messaging: Stable end-to-end encrypted direct messaging between network endpoints.Tauri Desktop Client: Refined native UI performance using system webviews.🐛 Bug Fixes & ImprovementsPerformance: Optimized backend memory usage in the core Rust engine.Networking: Resolved event-loop panics during unexpected peer disconnections.UI: Improved responsiveness across window resizes in the Tauri interface.📥 Downloads & AssetsPlatformAsset FilenameSHA-256 ChecksumLinux (AppImage)p2p_aggregator_X.Y.Z_amd64.AppImageYOUR_SHA256_HASH_HERELinux (Debian/Ubuntu)p2p_aggregator_X.Y.Z_amd64.debYOUR_SHA256_HASH_HEREWindows Installerp2p_aggregator_X.Y.Z_x64-setup.exeYOUR_SHA256_HASH_HERE💻 Installation & QuickstartLinux AppImageTo run the AppImage binary directly:chmod +x p2p_aggregator_X.Y.Z_amd64.AppImage
./p2p_aggregator_X.Y.Z_amd64.AppImage
Linux Debian / UbuntuTo install the .deb package:sudo dpkg -i p2p_aggregator_X.Y.Z_amd64.deb
sudo apt-get install -f # Resolve dependencies if needed
⚠️ Known Issues & LimitationsBootstrap Nodes: Public bootstrap node stability is still being evaluated; ensure your local firewall allows peer discovery traffic.Windows SmartScreen: Because binaries are currently unsigned, Windows Defender may display a warning prompt upon first launch. Click "More info" -> "Run anyway" to proceed.Thank you for testing and contributing to p2p_aggregator!
# p2p_aggregator

> A decentralized peer-to-peer media aggregation, RSS feed ingestion, and encrypted messaging application.

## 🚀 Features

- **Decentralized Networking:** Powered by `libp2p` for robust peer discovery and direct node-to-node communication.
- **Feed & Media Aggregation:** Ingest and sync RSS feeds and media updates locally across peers.
- **Secure Messaging:** End-to-end encrypted direct messaging between network nodes.
- **Lightweight UI:** Built with Tauri and an HTML/JS frontend for a native, low-resource desktop experience.
- **Memory Safe:** Core backend written entirely in Rust.

## 🛠️ Tech Stack

- **Core / P2P:** [Rust](https://www.rust-lang.org/), [libp2p](https://libp2p.io/)
- **Desktop Framework:** [Tauri](https://tauri.app/)
- **Frontend:** HTML, JavaScript

## 📦 Getting Started

### Prerequisites

- [Rust toolchain](https://www.rust-lang.org/tools/install) (stable)
- [Node.js](https://nodejs.org/) (for frontend dependencies / Tauri CLI)
- Tauri system dependencies (refer to the [Tauri Prerequisites Guide](https://tauri.app/v1/guides/getting-started/prerequisites))

### Building from Source

1. Clone the repository:
   ```bash
   git clone [https://github.com/your-username/p2p_aggregator.git](https://github.com/your-username/p2p_aggregator.git)
   cd p2p_aggregator
