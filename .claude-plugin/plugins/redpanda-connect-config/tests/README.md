# Test Suite for Redpanda Connect Plugin

Quality evaluations for the Claude Code plugin using [Promptfoo](https://www.promptfoo.dev/).

## Overview

This test suite evaluates LLM-generated outputs from the three main plugin skills:
- **Component Search** (`/rpcn:search`) - Finding the right Redpanda Connect components
- **Bloblang Authoring** (`/rpcn:blobl`) - Generating data transformation scripts
- **Pipeline Creation** (`/rpcn:pipeline`) - Building streaming configurations

Tests validate:
- **Functional correctness** - Does it work? (validated with `rpk` commands)
- **Quality** - Is it good? (LLM-as-judge + custom assertions)
- **Security** - Are secrets handled properly?
- **Modern components** - No deprecated components

## Prerequisites

```bash
# 1. Install rpk (Redpanda Connect CLI)
brew install redpanda  # macOS
# Linux/Other: https://docs.redpanda.com/redpanda-connect/

# 2. Install Promptfoo
npm install -g promptfoo

# 3. Set API key
export ANTHROPIC_API_KEY=<your-api-key>
```

## Running Tests

### Quick Start

```bash
cd .claude-plugin/plugins/redpanda-connect-config/tests
./run-tests.sh
```

This will:
1. Generate eval configs from JSON fixtures
2. Run all evaluations using Promptfoo
3. Report pass/fail results

### Individual Evaluation

Run a single skill's tests with custom filters:

```bash
# Generate and run specific evaluation
./generate-tests.py --type search --output evals/search-eval.yaml
cd evals && npx promptfoo eval -c search-eval.yaml

# Filter by difficulty level
./generate-tests.py --type blobl --filter difficulty=basic --output evals/blobl-basic.yaml
cd evals && npx promptfoo eval -c blobl-basic.yaml

# Limit test count
./generate-tests.py --type pipeline --count 10 --output evals/pipeline-sample.yaml
cd evals && npx promptfoo eval -c pipeline-sample.yaml
```

### View Results

```bash
cd evals/
npx promptfoo view
```

Opens a web UI showing pass/fail status, score breakdowns, LLM outputs, and assertion details.

## Architecture

### Fixture-Driven Generation

**Fixtures are the source of truth.** Test data lives in JSON files under `fixtures/`:

- `search_queries.json` - Component search queries with expected results
- `blobl_transformations.json` - Bloblang transformation scenarios with sample I/O
- `pipeline_descriptions.json` - Pipeline requirements with validation criteria

Each fixture includes:
- Test description and context
- Expected behavior or output
- Validation criteria
- Difficulty level (basic, intermediate, advanced, edge_case)

**Eval configs are auto-generated.** The `generate-tests.py` script converts fixtures into Promptfoo YAML configs with:
- JavaScript assertions for pattern matching
- LLM-as-judge rubrics for quality scoring
- Test structure and output paths

Generated files are git-ignored to keep fixtures as the single source of truth.

### Validation Strategy

**JavaScript Assertions** check specific patterns:
- Extract code blocks from LLM output
- Validate syntax and structure
- Check for deprecated components
- Ensure security best practices (env vars, no hardcoded secrets)

**LLM-as-Judge** evaluates subjective quality:
- Correctness and completeness
- Error handling
- Documentation clarity
- Overall helpfulness

**Shell Scripts** (planned) will validate with actual tools:
- `validate_blobl_script.sh` - Test scripts with `rpk connect blobl`
- `validate_pipeline_config.sh` - Lint configs with `rpk connect lint`

### Non-Deterministic Outputs

LLMs produce varied outputs. Tests handle this by:
- Checking functional equivalence, not exact matches
- Asserting properties that must hold (valid syntax, real functions)
- Using threshold scoring (pass if score > threshold)
- Validating with ground truth tools (`rpk` commands)

## Adding New Tests

Edit the appropriate fixture file:

```bash
vim fixtures/search_queries.json
vim fixtures/blobl_transformations.json
vim fixtures/pipeline_descriptions.json
```

Then run tests - eval configs regenerate automatically:

```bash
./run-tests.sh
```

### Fixture Format

```json
{
  "id": "unique-test-id",
  "description": "what this test validates",
  "query": "user's natural language request",
  "expected_components": ["component_name"],
  "validation_criteria": [
    "Uses correct components",
    "Handles errors properly",
    "Includes clear documentation"
  ],
  "difficulty": "intermediate"
}
```

⚠️ **Do not manually edit generated YAML files** - they are overwritten on each run. If you need custom assertion logic, enhance `generate-tests.py` instead.

## Success Criteria

### Functional Correctness (Required)
- Search: All suggested components exist in `rpk connect list`
- Bloblang: All scripts use real Bloblang functions
- Pipeline: All configs pass `rpk connect lint`

### Quality Targets
- Search relevance: >0.8 average score
- Bloblang correctness: >0.9 pass rate
- Pipeline security: 100% (no hardcoded secrets)
- Component modernity: 100% (no deprecated components)

### Security (Required)
- No plain-text passwords in configs
- All secrets use `${ENV_VAR}` format
- Includes `.env.example` file when applicable

### Deprecation Checks
- Use `kafka_franz` not deprecated `kafka` output
- Use `ockam_kafka` or `redpanda` for Kafka inputs
- Tests fail if deprecated components detected
