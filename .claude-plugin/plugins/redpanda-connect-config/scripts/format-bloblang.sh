#!/bin/bash
# Format bloblang functions and methods metadata into category files
# Usage: ./format-bloblang.sh --output-dir <dir>
# Example:
#   ./format-bloblang.sh --output-dir /tmp/blobl-session-abc123

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Process both functions and methods
for CATEGORY in bloblang-functions bloblang-methods; do
    rpk connect list --format jsonschema "$CATEGORY" | python3 "$SCRIPT_DIR/format-bloblang.py" "$@"
done
