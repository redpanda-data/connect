#!/bin/bash
# Validates a Redpanda Connect pipeline configuration
# Usage: ./validate-pipeline.sh <session_dir>
#
# Expected files in session_dir:
#   - pipeline.yaml (required) - The configuration to validate
#   - .env (optional) - Environment variables for validation

set -euo pipefail

SESSION_DIR="${1:?Error: SESSION_DIR argument required}"

# Validate session directory and files exist
if [[ ! -d "$SESSION_DIR" ]]; then
    echo "Error: SESSION_DIR '$SESSION_DIR' does not exist" >&2
    exit 1
fi
if [[ ! -f "$SESSION_DIR/pipeline.yaml" ]]; then
    echo "Error: $SESSION_DIR/pipeline.yaml not found" >&2
    exit 1
fi

# Load environment variables if .env exists
if [[ -f "$SESSION_DIR/.env" ]]; then
    echo "Loading environment variables from .env..." >&2
    set -a
    source "$SESSION_DIR/.env"
    set +a
fi

# Run validation
rpk connect lint "$SESSION_DIR/pipeline.yaml"
