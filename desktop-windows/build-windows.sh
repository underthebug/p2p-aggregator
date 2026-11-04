#!/usr/bin/env bash
# Cross-compiles the Windows desktop app from Linux and packages a portable folder + zip.
# Prerequisites: rustup target x86_64-pc-windows-msvc, cargo-xwin, cargo-tauri, clang,
# llvm-lib/llvm-rc/llvm-ar on PATH, and `lld-link` (symlink to rust-lld) on PATH.
set -euo pipefail

cd "$(dirname "$0")"
export XWIN_ACCEPT_LICENSE=1
TARGET=x86_64-pc-windows-msvc
OUT=dist/p2p-desktop-windows

mkdir -p stage
(cd core && cargo xwin build --release --target "$TARGET" --bin p2p-aggregator)
cp "core/target/$TARGET/release/p2p-aggregator.exe" stage/

(cd src-tauri && cargo xwin build --release --target "$TARGET" --bin p2p-daemon)
cp "src-tauri/target/$TARGET/release/p2p-daemon.exe" stage/

(cd src-tauri && cargo tauri build --runner cargo-xwin --target "$TARGET" --no-bundle)

rm -rf "$OUT"
mkdir -p "$OUT"
cp "src-tauri/target/$TARGET/release/p2p-desktop.exe" stage/p2p-aggregator.exe stage/p2p-daemon.exe "$OUT/"
(cd dist && rm -f p2p-desktop-windows.zip && python3 -c "import shutil; shutil.make_archive('p2p-desktop-windows', 'zip', '.', 'p2p-desktop-windows')")
echo "Built dist/p2p-desktop-windows.zip"
