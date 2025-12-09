#!/bin/bash
# Custom Promptfoo assertion: Extract and validate pipeline config from LLM output
#
# Usage: ./validate_pipeline_config.sh <output_text>
# Returns: 0 if valid, 1 if invalid

set -e

OUTPUT_TEXT="$1"

# Create temp directory
TEMP_DIR=$(mktemp -d -t rpcn-promptfoo-XXXXXX)
trap "rm -rf $TEMP_DIR" EXIT

# Extract YAML from markdown code block
# Look for ```yaml or ```yml code blocks
CONFIG=$(echo "$OUTPUT_TEXT" | sed -n '/```\(yaml\|yml\)/,/```/p' | sed '1d;$d')

if [ -z "$CONFIG" ]; then
    echo "❌ No YAML config found in output" >&2
    exit 1
fi

# Write config to temp file
echo "$CONFIG" > "$TEMP_DIR/pipeline.yaml"

# Check for hardcoded secrets (basic check)
if grep -qE '(password|secret|key).*:.*[a-zA-Z0-9]{8,}' "$TEMP_DIR/pipeline.yaml"; then
    # Check if they use environment variables
    if ! grep -q '\${' "$TEMP_DIR/pipeline.yaml"; then
        echo "⚠️  Warning: Possible hardcoded secrets detected" >&2
        # Don't fail, just warn
    fi
fi

# Get plugin root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_ROOT="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"

# Validate using validate-pipeline.sh
if "$PLUGIN_ROOT/scripts/validate-pipeline.sh" "$TEMP_DIR" >/dev/null 2>&1; then
    echo "✓ Pipeline config is valid"
    exit 0
else
    echo "❌ Pipeline config validation failed" >&2
    exit 1
fi
