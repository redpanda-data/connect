#!/bin/bash
# Test a Bloblang script with input data
# Usage: ./test-blobl.sh <session_dir>
#
# Expected files in session_dir:
#   - data.json: Input JSON data (one line per message)
#   - script.blobl: Bloblang transformation script

set -euo pipefail

SESSION_DIR="${1:?Error: SESSION_DIR argument required}"

# Validate session directory and files exist
if [[ ! -d "$SESSION_DIR" ]]; then
    echo "Error: SESSION_DIR '$SESSION_DIR' does not exist" >&2
    exit 1
fi
if [[ ! -f "$SESSION_DIR/data.json" ]]; then
    echo "Error: $SESSION_DIR/data.json not found" >&2
    exit 1
fi
if [[ ! -f "$SESSION_DIR/script.blobl" ]]; then
    echo "Error: $SESSION_DIR/script.blobl not found" >&2
    exit 1
fi

# Compact JSON with jq and pipe to rpk connect blobl
jq -c < "$SESSION_DIR/data.json" | rpk connect blobl --pretty -f "$SESSION_DIR/script.blobl"
