#!/bin/bash
# Get rpk connect version number
# Usage: ./rpk-version.sh
# Output: Version number (e.g., "4.72.0")

set -euo pipefail

rpk connect --version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1
