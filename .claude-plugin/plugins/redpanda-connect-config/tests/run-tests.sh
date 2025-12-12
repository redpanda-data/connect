#!/bin/bash
# Test runner script for Redpanda Connect Claude Code plugin
# Runs Promptfoo evaluations for all three commands
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_ROOT="$(dirname "$SCRIPT_DIR")"
EVALS_DIR="$SCRIPT_DIR/evals"

echo "====================================="
echo "Redpanda Connect Plugin Test Suite"
echo "====================================="
echo ""

# Check prerequisites
echo "Checking prerequisites..."

# Check rpk
if ! command -v rpk &> /dev/null; then
    echo "❌ Error: rpk is required but not installed"
    echo "   Install Redpanda Connect from: https://docs.redpanda.com/redpanda-connect/"
    exit 1
fi

# Check promptfoo
if ! command -v promptfoo &> /dev/null; then
    echo "❌ Error: promptfoo is required but not installed"
    echo "   Install with: npm install -g promptfoo"
    exit 1
fi

# Check ANTHROPIC_API_KEY
if [ -z "$ANTHROPIC_API_KEY" ]; then
    echo "❌ Error: ANTHROPIC_API_KEY environment variable not set"
    echo "   Set with: export ANTHROPIC_API_KEY=<your-api-key>"
    exit 1
fi

RPK_VERSION=$(rpk version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "unknown")
PROMPTFOO_VERSION=$(promptfoo --version 2>/dev/null || echo "unknown")

echo "✓ rpk version: $RPK_VERSION"
echo "✓ promptfoo version: $PROMPTFOO_VERSION"
echo "✓ ANTHROPIC_API_KEY is set"
echo ""

# Generate eval configs from fixtures
echo "======================================"
echo "Generating Eval Configs"
echo "======================================"
echo ""

echo "Generating search-eval.yaml..."
"$SCRIPT_DIR/generate-tests.py" --type search --output "$EVALS_DIR/search-eval.yaml"

echo "Generating blobl-eval.yaml..."
"$SCRIPT_DIR/generate-tests.py" --type blobl --output "$EVALS_DIR/blobl-eval.yaml"

echo "Generating pipeline-eval.yaml..."
"$SCRIPT_DIR/generate-tests.py" --type pipeline --output "$EVALS_DIR/pipeline-eval.yaml"

echo ""
echo "✓ Eval configs generated"
echo ""

# Navigate to evals directory
cd "$EVALS_DIR"

# Run evaluations
TOTAL_RESULT=0

# Function to run eval and track results
run_eval() {
    local config_file=$1
    local name=$2

    echo "====================================="
    echo "Running $name Evaluations"
    echo "====================================="
    echo ""

    if promptfoo eval -c "$config_file"; then
        echo ""
        echo "✓ $name evaluations completed"
        echo ""
    else
        echo ""
        echo "❌ $name evaluations failed"
        echo ""
        TOTAL_RESULT=1
    fi
}

# Run all three eval configs
run_eval "search-eval.yaml" "Search Command"
run_eval "blobl-eval.yaml" "Bloblang Command"
run_eval "pipeline-eval.yaml" "Pipeline Command"

echo "====================================="

if [ $TOTAL_RESULT -eq 0 ]; then
    echo "✓ All evaluations completed!"
    echo "====================================="
    echo ""
    echo "Results saved to:"
    echo "  - $SCRIPT_DIR/promptfoo-search-results.json"
    echo "  - $SCRIPT_DIR/promptfoo-blobl-results.json"
    echo "  - $SCRIPT_DIR/promptfoo-pipeline-results.json"
    echo ""
    echo "View results: cd $EVALS_DIR && promptfoo view"
    exit 0
else
    echo "❌ Some evaluations failed"
    echo "====================================="
    echo ""
    echo "View detailed results: cd $EVALS_DIR && promptfoo view"
    exit 1
fi
