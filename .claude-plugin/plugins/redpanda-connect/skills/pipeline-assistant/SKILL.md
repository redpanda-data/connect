---
name: pipeline-assistant
description: This skill should be used when users need to create or fix Redpanda Connect pipeline configurations. Trigger when users mention "config", "pipeline", "YAML", "create a config", "fix my config", "validate my pipeline", or describe a streaming pipeline need like "read from Kafka and write to S3".
---

# Redpanda Connect Configuration Assistant

Create working, validated Redpanda Connect configurations from scratch or repair existing configurations that have issues.

**This skill REQUIRES skills: `component-search`, `bloblang-authoring`.**

## Objective

Deliver a complete, valid YAML configuration that passes validation and meets the user's requirements.
Whether starting from a description or fixing a broken config, the result must be production-ready with properly secured credentials.

Handle Two Scenarios:
**Creation** - User provides description like "Read from Kafka on localhost:9092 topic 'events' to stdout"
**Repair** - User provides config file path and optional error context

This skill focuses ONLY on pipeline configuration orchestration and validation.

**Skill Delegation**:

NEVER directly use component-search or bloblang-authoring tools.
- **Component Discovery** - ALWAYS delegate to `component-search` skill when it is unclear which components to use OR when you need component configuration details
- **Bloblang Development** - ALWAYS delegate to `bloblang-authoring` skill when creating or fixing Bloblang transformations and NEVER write Bloblang yourself

## Setup

This skill requires: `rpk`, `rpk connect`.
See the [SETUP](SETUP.md) for installation instructions.

## Tools

### Scaffold Pipeline

Generates YAML configuration template from component expression.
Useful for quickly creating first pipeline draft.

```bash
# Usage:
rpk connect create [--small] <input>,...[/<processor>,...]/<output>,...

# Examples:
rpk connect create stdin/bloblang,awk/nats
rpk connect create file,http_server/protobuf/http_client  # Multiple inputs
rpk connect create kafka_franz/stdout  # Only input and output, no processors
rpk connect create --small stdin/bloblang/stdout  # Minimal config, omit advanced fields
```
- Requires component expression specifying desired inputs, processors, and outputs
- Expression format: `inputs/processors/outputs` separated by `/`
- Multiple components of same type separated by `,`
- Outputs complete YAML configuration with specified components
- `--small` flag omits advanced fields

### Online Component Documentation

Use the `component-search` skill's `Online Component Documentation` tool to look up detailed configuration information for any Redpanda Connect component containing usage examples, field descriptions, and best practices.

### Lint Pipeline

Validates Redpanda Connect pipeline configurations.

```bash
# Usage:
rpk connect lint [--env-file <.env>] <pipeline.yaml>

# Examples:
rpk connect lint --env-file ./.env ./pipeline.yaml
rpk connect lint pipeline-without-secrets.yaml
```
- Requires pipeline configuration file path (e.g., `pipeline.yaml`)
- Optional `--env-file` flag provides `.env` file for environment variable substitution
- Validates YAML syntax, component configurations, and Bloblang expressions
- Outputs detailed error messages with specific location information
- Exit code `0` indicates success, non-zero indicates validation failures
- Can be run repeatedly during pipeline development and iteration

### Run Pipeline

Executes Redpanda Connect pipeline to test end-to-end functionality.

```bash
# Usage:
rpk connect run [--log.level DEBUG] --env-file <.env> <pipeline.yaml>

# Examples:
rpk connect run pipeline-without-secrets.yaml
rpk connect run --env-file ./.env ./pipeline.yaml  # With secrets
rpk connect run --log.level DEBUG --env-file ./.env ./pipeline.yaml  # With debug logging
```
- Requires pipeline configuration file path (e.g., `pipeline.yaml`)
- Optional `--env-file` flag provides dotenv file for environment variable substitution
- Optional `--log.level DEBUG` enables detailed logging for troubleshooting connection and processing issues
- Starts pipeline and maintains active connections to inputs and outputs
- Runs continuously until manually terminated with Ctrl+C (SIGINT)
- Can be run repeatedly during pipeline development and iteration

### Test with Standard Input/Output

Test pipeline logic with `stdin`/`stdout` before connecting to real systems.
Especially useful for validating routing logic, error handling, and transformations.

**Example: Content-based routing**

```yaml
input:
  stdin: {}

pipeline:
  processors:
    - mapping: |
        root = this
        # Route based on message type
        if this.type == "error" {
          meta route = "dlq"
        } else if this.priority == "high" {
          meta route = "urgent"
        } else {
          meta route = "standard"
        }

output:
  switch:
    cases:
      - check: 'meta("route") == "dlq"'
        output:
          stdout: {}
        processors:
          - mapping: 'root = "DLQ: " + content().string()'

      - check: 'meta("route") == "urgent"'
        output:
          stdout: {}
        processors:
          - mapping: 'root = "URGENT: " + content().string()'

      - check: 'meta("route") == "standard"'
        output:
          stdout: {}
        processors:
          - mapping: 'root = "STANDARD: " + content().string()'
```

**Test all routes:**
```bash
echo '{"type":"error","msg":"failed"}' | rpk connect run test.yaml
# Output: DLQ: {"type":"error","msg":"failed"}

echo '{"priority":"high","msg":"urgent"}' | rpk connect run test.yaml
# Output: URGENT: {"priority":"high","msg":"urgent"}

echo '{"priority":"low","msg":"normal"}' | rpk connect run test.yaml
# Output: STANDARD: {"priority":"low","msg":"normal"}
```

**Limitations:**
- Stdin/stdout cannot test batching behavior realistically
- No connection, retry, or timeout logic validation
- Cannot test ordering guarantees or parallel processing
- Real integration testing still required before production deployment

## YAML Configuration Structure

Top-level keys:
- `input` - Data source (required): kafka_franz, http_server, stdin, aws_s3, etc
- `output` - Data destination (required): kafka_franz, postgres, stdout, aws_s3, etc
- `pipeline.processors` - Transformations (optional, execute sequentially)
- `cache_resources`, `rate_limit_resources` - Reusable components (optional)

**Environment variables (required for secrets):**
```yaml
# Basic reference
broker: "${KAFKA_BROKER}"

# With default value
broker: "${KAFKA_BROKER:localhost:9092}"
```

**Field type conventions:**
- Durations: `"30s"`, `"5m"`, `"1h"`, `"100ms"`
- Sizes: `"5MB"`, `"1GB"`, `"512KB"`
- Booleans: `true`, `false` (no quotes)

**Minimal example:**
```yaml
input:
  redpanda:
    seed_brokers: ["${KAFKA_BROKER}"]
    topics: ["${TOPIC}"]

pipeline:
  processors:
    - mapping:
        | # Bloblang transformation - use  bloblang-authoring skill to create
        root = this
        root.timestamp = now()

output:
  stdout: {}
```

Use `Quick Pipeline Scaffolding` for initial drafts.

### Production Recipes/Patterns

The `./resources/recipes/` directory contains validated production patterns.
Each recipe includes:
- **Markdown documentation** (`.md`) - Pattern explanation, configuration details, testing instructions, and variations
- **Working YAML configuration** (`.yaml`) - Complete, tested pipeline referenced in the markdown

**Before writing pipelines:**
1. **Read component documentation** - Use `Online Component Documentation` tool for detailed field info and examples
2. **Read relevant recipes** - When user describes a pattern matching a recipe (routing, DLQ, replication, etc.), read the markdown file first
3. **Adapt, don't copy** - Use recipes as reference for patterns and best practices, customize for user's specific requirements

#### Available Recipes
**Error Handling**
- `dlq-basic.md` - Dead letter queue for error handling

**Routing**
- `content-based-router.md` - Route messages by field values
- `multicast.md` - Fan-out to multiple destinations

**Replication**
- `kafka-replication.md` - Cross-cluster Kafka streaming
- `cdc-replication.md` - Database change data capture

**Cloud Storage**
- `s3-sink-basic.md` - S3 output with batching
- `s3-sink-time-based.md` - Time-partitioned S3 writes
- `s3-polling.md` - Poll S3 for new files

**Stateful Processing**
- `stateful-counter.md` - Stateful counting with cache
- `window-aggregation.md` - Time-window aggregations

**Performance & Monitoring**
- `rate-limiting.md` - Throughput control
- `custom-metrics.md` - Prometheus metrics

## Workflow

### Creating New Configurations

1. **Understand requirements**
   - Parse description for source, destination, transformations, and special needs (ordering, batching, etc.)
   - Ask clarifying questions for ambiguous aspects
   - Check `./resources/recipes/` for relevant patterns

2. **Discover components**
   - Use `component-search` skill if unclear which components to use
   - Read component documentation for configuration details

3. **Build configuration**
   - Generate scaffold with `rpk connect create input/processor/output`
   - Add all required fields from component schemas
   - For secrets: ask user for env var names → use `${VAR_NAME}` → document in `.env.example`
   - Keep configuration minimal and simple

4. **Add transformations** (if needed)
   - Delegate to `bloblang-authoring` skill for tested scripts
   - Embed in `pipeline.processors` section

5. **Validate and iterate**
   - Run `rpk connect lint`
   - On errors: parse → fix → re-validate until clean
   - Iterate until validation passes

6. **Test and iterate**
   - Test with `rpk connect run`
     - Temporarily use `stdin` and `stdout` for easier testing
     - Run with `rpk connect run`
     - Fix any runtime issues
     - Test all edge cases
     - Iterate until tests pass
   - Test connection and authentication to real systems if possible

7. **Deliver**
   - Deliver final `pipeline.yaml` and `.env.example`
   - Explain component choices and configuration decisions
   - Create concise `TESTING.md` with only practical followup testing instructions:
     - How to set up environment
     - Command to run the pipeline
     - Sample curl/test commands with realistic data
     - How to verify results in the target system
     - ONLY include new/essential information, avoid verbose explanations
   - NEVER create README files
   - Show concise summary in chat response

### Repairing Existing Configurations

1. **Diagnose**
   - Run `rpk connect lint` to identify errors
   - Review user-provided context about symptoms
   - Find root causes (typos, deprecations, type mismatches)

2. **Explain issues**
   - Translate validation errors to plain language
   - Explain why current configuration doesn't work
   - Identify root causes, not just symptoms

3. **Fix minimally**
   - Get user approval before modifying files
   - Preserve original structure, comments, and intent
   - Replace deprecated components if needed
   - Apply secret handling with environment variables

4. **Verify**
   - Re-validate after each change
   - Test modified Bloblang transformations
   - Confirm no regressions introduced

### Security Requirements (Critical)

**Never store credentials in plain text:**
- All passwords, secrets, tokens, API keys MUST use `${ENV_VAR}` syntax in YAML
- Never put actual credentials in YAML or conversation

**Environment variable files:**
- `.env` - Contains actual secret values, used at runtime with `--env-file .env`, NEVER commit to git
- `.env.example` - Documents required variables with placeholder values, safe to commit
- Always remind user to add `.env` to `.gitignore`

**When encountering sensitive fields** (from `<secret_fields>` in component schema):
1. Ask user for environment variable name (e.g., `KAFKA_PASSWORD`)
2. Write `${KAFKA_PASSWORD}` in YAML configuration
3. Document in `.env.example`: `KAFKA_PASSWORD=your_password_here`
4. User creates actual `.env` with real value: `KAFKA_PASSWORD=actual_secret_123`
