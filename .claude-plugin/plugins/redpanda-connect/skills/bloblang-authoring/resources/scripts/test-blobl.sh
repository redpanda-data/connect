#!/bin/bash
# Test a Bloblang script with input data
# Usage: ./test-blobl.sh <directory>
#
# Expected files in directory:
#   - data.json: Input JSON data (one line per message)
#   - script.blobl: Bloblang transformation script

set -euo pipefail

DIR="${1:?Error: DIR argument required}"

# Validate directory and files exist
if [[ ! -d "$DIR" ]]; then
    echo "Error: directory '$DIR' does not exist" >&2
    exit 1
fi
if [[ ! -f "$DIR/data.json" ]]; then
    echo "Error: $DIR/data.json not found" >&2
    exit 1
fi
if [[ ! -f "$DIR/script.blobl" ]]; then
    echo "Error: $DIR/script.blobl not found" >&2
    exit 1
fi

# Compact JSON with jq and pipe to rpk connect blobl
jq -c < "$DIR/data.json" | rpk connect blobl --pretty -f "$DIR/script.blobl"
