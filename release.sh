#!/usr/bin/env bash
# Builds, packages and (optionally) publishes a Linux release in one command.
#
#   ./release.sh 0.2.2              build + test + package into dist/linux-v0.2.2/
#   ./release.sh 0.2.2 --publish    also commit, tag, push and create a GitHub Release
#   ./release.sh 0.2.2 --skip-tests --skip-build   quick packaging check
#
# Before running: add a "## <version>" section to CHANGELOG.md.
# Publishing needs git credentials and the GitHub CLI (gh auth login). On a
# machine without them, run without --publish and publish from another machine.
set -euo pipefail

VERSION="${1:-}"
shift || true
PUBLISH=0 SKIP_TESTS=0 SKIP_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --publish) PUBLISH=1 ;;
    --skip-tests) SKIP_TESTS=1 ;;
    --skip-build) SKIP_BUILD=1 ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Usage: $0 X.Y.Z [--publish] [--skip-tests] [--skip-build]" >&2; exit 2; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TAG="linux-v$VERSION"
OUT="$ROOT/dist/$TAG"
export PATH="$HOME/.cargo/bin:$PATH"
step() { printf '\n==> %s\n' "$*"; }

step "Checking prerequisites"
NOTES="$(awk -v v="## $VERSION" '$0==v{f=1;next} /^## /{f=0} f' "$ROOT/CHANGELOG.md" | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}')"
[[ -n "${NOTES//[[:space:]]/}" ]] || { echo "CHANGELOG.md has no '## $VERSION' section. Add one first." >&2; exit 1; }

if [[ $PUBLISH -eq 1 ]]; then
  command -v gh >/dev/null || { echo "gh (GitHub CLI) is not installed; cannot --publish." >&2; exit 1; }
  gh auth status >/dev/null 2>&1 || { echo "gh is not signed in (gh auth login)." >&2; exit 1; }
  git -C "$ROOT" rev-parse "$TAG" >/dev/null 2>&1 && { echo "Tag $TAG already exists. Pick a new version." >&2; exit 1; }
fi

step "Setting version $VERSION"
sed -i "0,/^version = \".*\"/s//version = \"$VERSION\"/" "$ROOT/Cargo.toml" "$ROOT/src-tauri/Cargo.toml"
sed -i "0,/\"version\": \".*\"/s//\"version\": \"$VERSION\"/" "$ROOT/src-tauri/tauri.conf.json"

# The root crate cannot build inside a folder whose parent has an unrelated Cargo.toml,
# so it is built from a clean temp copy that contains only the root crate.
ROOT_BUILD="$(mktemp -d)"
trap 'rm -rf "$ROOT_BUILD"' EXIT
rsync -a --exclude target --exclude .git --exclude src-tauri --exclude ui --exclude dist \
  --exclude desktop-windows --exclude data --exclude '*.db' --exclude '*:*' --exclude '{' --exclude 0 \
  "$ROOT/" "$ROOT_BUILD/"

if [[ $SKIP_TESTS -eq 0 ]]; then
  step "Running tests"
  (cd "$ROOT_BUILD" && cargo test)
  (cd "$ROOT/src-tauri" && cargo test)
fi

if [[ $SKIP_BUILD -eq 0 ]]; then
  step "Building"
  (cd "$ROOT_BUILD" && cargo build --release --bin p2p-aggregator)
  (cd "$ROOT/src-tauri" && cargo build --release --bin p2p-daemon && cargo tauri build --no-bundle)
fi

step "Packaging"
mkdir -p "$OUT"
STAGE="$(mktemp -d)"
cp "$ROOT_BUILD/target/release/p2p-aggregator" "$STAGE/"
cp "$ROOT/src-tauri/target/release/p2p-daemon" "$ROOT/src-tauri/target/release/p2p-desktop" "$STAGE/"
cp "$ROOT/CHANGELOG.md" "$STAGE/"
for bin in p2p-daemon p2p-desktop; do
  grep -qa "$VERSION" "$STAGE/$bin" || { echo "$bin does not contain version $VERSION. Run without --skip-build." >&2; exit 1; }
done
ARCHIVE="mesh-station-linux-$VERSION.tar.gz"
tar -C "$STAGE" -czf "$OUT/$ARCHIVE" .
rm -rf "$STAGE"
(cd "$OUT" && sha256sum "$ARCHIVE" > SHA256SUMS.txt)
cat "$OUT/SHA256SUMS.txt"

if [[ $PUBLISH -eq 0 ]]; then
  printf '\nDone. Nothing was published. Files are in %s\n' "$OUT"
  exit 0
fi

step "Committing, tagging and publishing"
git -C "$ROOT" add -A CHANGELOG.md Cargo.toml src src-tauri ui
git -C "$ROOT" commit -m "Release $VERSION" -m "Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>"
git -C "$ROOT" tag -a "$TAG" -m "Linux $VERSION"
git -C "$ROOT" push origin "$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)" "$TAG"
printf '%s\n\nExtract the archive and run p2p-desktop. Verify downloads against SHA256SUMS.txt.\n' "$NOTES" > "$OUT/notes.md"
gh release create "$TAG" "$OUT/$ARCHIVE" "$OUT/SHA256SUMS.txt" --title "Linux $VERSION" --notes-file "$OUT/notes.md"
printf '\nReleased %s\n' "$TAG"
