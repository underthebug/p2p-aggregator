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
