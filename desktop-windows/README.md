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

For Linux cross-compilation, see `build-windows.sh` for the required `cargo-xwin`
and LLVM toolchain.
