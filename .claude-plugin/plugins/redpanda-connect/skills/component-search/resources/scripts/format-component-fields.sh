#!/bin/bash
# Format component fields from jsonschema output into tagged sections
# Usage: ./format-component-fields.sh <category> <component>
# Example: ./format-component-fields.sh inputs kafka_franz

set -euo pipefail

CATEGORY="$1"  # e.g., "inputs", "outputs", "processors"
COMPONENT="$2"  # e.g., "kafka_franz", "stdout"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Fetch jsonschema and pipe to Python formatter
# Note: rpk returns schema for ALL components regardless of component name argument
# Pass component name to Python script for filtering
rpk connect list --format jsonschema "${CATEGORY}" | python3 "$SCRIPT_DIR/format-component-fields.py" "$COMPONENT"
