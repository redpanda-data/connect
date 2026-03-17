#!/bin/bash
# Format bloblang functions and methods metadata into category files
# Usage: ./format-bloblang.sh
# Automatically uses skill resources cache directory

set -euo pipefail

# Get script directory and skill root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Create output directory in skill resources
OUTPUT_DIR="$SKILL_ROOT/resources/cache/bloblref/$("$SCRIPT_DIR/rpk-version.sh")"
mkdir -p "$OUTPUT_DIR"
echo "$OUTPUT_DIR"

# Process both functions and methods
for CATEGORY in bloblang-functions bloblang-methods; do
    rpk connect list --format jsonschema "$CATEGORY" | python3 "$SCRIPT_DIR/format-bloblang.py" --output-dir "$OUTPUT_DIR"
done
