#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v rustup >/dev/null 2>&1; then
  echo "rustup is required. Install from https://rustup.rs/" >&2
  exit 1
fi

rustup toolchain install 1.98.1 --profile default --component rustfmt --component clippy
cargo verify

echo "Tachyon workspace verified. See docs/DEVELOPMENT_WORKFLOW.md for local development checks."
