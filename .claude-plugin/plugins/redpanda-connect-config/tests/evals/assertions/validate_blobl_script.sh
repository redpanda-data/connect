#!/bin/bash
# Custom Promptfoo assertion: Extract and validate Bloblang script from LLM output
#
# Usage: ./validate_blobl_script.sh <output_text> [sample_data_json]
# Returns: 0 if valid, 1 if invalid

set -e

OUTPUT_TEXT="$1"
SAMPLE_DATA="${2:-{\"test\": \"data\"}}"

# Create temp directory
TEMP_DIR=$(mktemp -d -t rpcn-promptfoo-XXXXXX)
trap "rm -rf $TEMP_DIR" EXIT

# Extract Bloblang script from markdown code block
# Look for ```blobl or ```bloblang code blocks
SCRIPT=$(echo "$OUTPUT_TEXT" | sed -n '/```\(blobl\|bloblang\)/,/```/p' | sed '1d;$d')

if [ -z "$SCRIPT" ]; then
    echo "❌ No Bloblang script found in output" >&2
    exit 1
fi

# Write script to temp file
echo "$SCRIPT" > "$TEMP_DIR/script.blobl"

# Write sample data
echo "$SAMPLE_DATA" > "$TEMP_DIR/data.json"

# Get plugin root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_ROOT="$(dirname "$(dirname "$(dirname "$SCRIPT_DIR")")")"

# Validate using test-blobl.sh
if "$PLUGIN_ROOT/scripts/test-blobl.sh" "$TEMP_DIR" >/dev/null 2>&1; then
    echo "✓ Bloblang script is valid"
    exit 0
else
    echo "❌ Bloblang script validation failed" >&2
    exit 1
fi
