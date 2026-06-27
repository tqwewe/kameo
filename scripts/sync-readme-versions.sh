#!/usr/bin/env bash
# Sync the hardcoded `kameo = "X.Y"` version examples in the docs to match the
# current kameo package version in Cargo.toml. release-plz updates Cargo.toml
# versions but not these doc strings, so this runs as a follow-up step inside the
# release PR. Safe to re-run: it is a no-op when everything is already in sync.

set -euo pipefail

cd "$(dirname "$0")/.."

# Extract the kameo package version (the first `version` under [package]).
VERSION=$(awk '/^\[package\]/{p=1} p && /^version = /{gsub(/[" ]/,"",$3); print $3; exit}' Cargo.toml)
if [ -z "$VERSION" ]; then
  echo "error: could not determine kameo version from Cargo.toml" >&2
  exit 1
fi

# Docs pin the major.minor version only (e.g. 0.21).
MAJOR_MINOR="${VERSION%.*}"
echo "syncing docs to kameo = \"$MAJOR_MINOR\""

perl -i -pe 's/kameo = "[^"]*"/kameo = "'"$MAJOR_MINOR"'"/' README.md
perl -i -pe 's/kameo = \{ version = "[^"]*", features = \["console"\] \}/kameo = { version = "'"$MAJOR_MINOR"'", features = ["console"] }/' README.md
perl -i -pe 's/kameo = \{ version = "[^"]*", features = \["console"\] \}/kameo = { version = "'"$MAJOR_MINOR"'", features = ["console"] }/' console/README.md
perl -i -pe 's/kameo = "[^"]*"/kameo = "'"$MAJOR_MINOR"'"/' docs/getting-started.mdx
