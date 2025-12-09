---
name: rpcn:pipeline
description: Create or repair Redpanda Connect configurations with interactive guidance and validation
arguments:
  - name: target
    description: Either a natural language description (for new configs) or a file path (for fixing existing configs)
    required: true
  - name: context
    description: Optional context when fixing (e.g., "getting connection timeout", "deprecated component")
    required: false
---

# Redpanda Connect Configuration Assistant

Create working, validated Redpanda Connect configurations from scratch or repair existing configurations that have issues.

## Objective

Deliver a complete, valid YAML configuration that passes validation and meets the user's requirements. Whether starting from a description or fixing a broken config, the result must be production-ready with properly secured credentials.

## Context

**Configuration Structure:**

Redpanda Connect configs consist of:
- **Input** - Source of data (one required)
- **Pipeline** - Transformations, filters (optional)
- **Output** - Destination for data (one required)
- **Resources** - Reusable caches, rate limits, etc. (optional)

**Two Scenarios:**

1. **Creation** - User provides description like "Read from Kafka on localhost:9092 topic 'events' to stdout"
2. **Repair** - User provides config file path and optional error context

**Session Pattern:**

Create a temporary session directory to hold working files:
```bash
# Create temp directory (important: save the path for use throughout)
SESSION_DIR=$(mktemp -d -t rpcn-pipeline-XXXXXX)
# Creates: /tmp/rpcn-pipeline-XXXXXX/ (or system equivalent)
```

The session directory contains:
- `pipeline.yaml` - The configuration being created or repaired (written here for validation)

Clean up when done:
```bash
rm -rf "$SESSION_DIR"
```

**Available Commands:**

- `/rpcn:search <query>` - Discover components semantically
- `/rpcn:blobl <description>` - Create and test Bloblang transformations

**Available Resources:**

**format-component-fields.sh** - Retrieves component configuration schemas
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh <category> <component>

# Examples:
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh input kafka_franz
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh output postgres
```
- Categories (singular): input, output, processor, cache, rate-limit, buffer, metric, tracer
- Output includes structured field information:
  - `<required_fields>` - Must be configured
  - `<optional_fields>` - Common settings
  - `<advanced_fields>` - Tuning options
  - `<secret_fields>` - Credentials needing environment variables
- Automatically filters deprecated fields

**rpk-version.sh** - Returns current Connect version
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/rpk-version.sh

# Output: version string (e.g., "4.70.0")
```
- Used for documentation URL construction
- Pattern: `https://github.com/redpanda-data/connect/blob/v{version}/docs/modules/components/pages/{category}/{component}.adoc`

**validate-pipeline.sh** - Validates pipeline configurations with environment variables
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/validate-pipeline.sh <directory>

# Example:
${CLAUDE_PLUGIN_ROOT}/scripts/validate-pipeline.sh /tmp/my-session
```

The script expects these files in the target directory:
- `pipeline.yaml` (required) - The configuration to validate
- `.env` (optional) - Environment variables for validation

**YAML Essentials:**

Configuration structure - top-level keys:
- `input` - Data source (required): kafka_franz, http_server, stdin, aws_s3, etc.
- `output` - Data destination (required): kafka_franz, postgres, stdout, aws_s3, etc.
- `pipeline.processors` - Transformations (optional, execute sequentially)
- `cache_resources`, `rate_limit_resources` - Reusable components (optional)

Environment variables (required for secrets):
```yaml
# Basic reference
broker: "${KAFKA_BROKER}"

# With default value
broker: "${KAFKA_BROKER:localhost:9092}"
```

Field type conventions:
- Durations: `"30s"`, `"5m"`, `"1h"`, `"100ms"`
- Sizes: `"5MB"`, `"1GB"`, `"512KB"`
- Booleans: `true`, `false` (no quotes)

Minimal example:
```yaml
input:
  kafka_franz:
    seed_brokers: ["${KAFKA_BROKER}"]
    topics: ["${TOPIC}"]

pipeline:
  processors:
    - mapping: |  # Bloblang transformation - use /rpcn:blobl to create
        root = this
        root.timestamp = now()

output:
  stdout: {}
```

**Production Patterns:**

See `recipes/` directory of validated patterns:

**Error Handling**
- `dlq-basic.md` - Route invalid messages to dead letter queue

**Routing**
- `content-based-router.md` - Filter and route by field values
- `multicast.md` - Send messages to multiple destinations

**Replication**
- `kafka-replication.md` - Cross-cluster Kafka replication
- `cdc-replication.md` - Database change data capture streaming

**Cloud Storage**
- `s3-sink-basic.md` - Write to S3 with batching
- `s3-sink-time-based.md` - Time-based S3 partitioning
- `s3-polling.md` - Poll S3 for new files

**Stateful Processing**
- `stateful-counter.md` - Track counts with circuit breaker
- `window-aggregation.md` - Time-window aggregations

**Performance & Monitoring**
- `rate-limiting.md` - Control throughput to downstream systems
- `custom-metrics.md` - Emit custom Prometheus metrics

All recipes include complete YAML, inline comments, and testing instructions.

## Requirements

### For All Configurations

1. **Security first** - Never store credentials in plain text:
   - All passwords, secrets, tokens, API keys must use environment variables
   - Create .env.example file documenting required variables
   - Remind user to add .env to .gitignore

   **When encountering sensitive fields** (from `<secret_fields>` section):
   1. Ask user for environment variable name
   2. Use `${VAR_NAME}` syntax in YAML
   3. Document in .env.example with description
   4. Never put actual credentials in YAML or conversation

2. **Validate thoroughly** - Configurations must be error-free:
   - Pass `validate-pipeline.sh` validation
   - Have all required fields populated
   - Use non-deprecated components
   - Contain tested Bloblang transformations

   **When validation or testing fails**:
   1. Parse error messages
   2. Identify root cause
   3. Apply fix
   4. Re-validate
   5. Iterate until successful

3. **Guide interactively** - Gather all necessary information:
   - Prompt for required fields not provided
   - Ask clarifying questions about ambiguous requirements
   - Confirm sensitive field handling approach
   - Get user approval before applying fixes to existing files

### When Creating New Configurations

1. **Understand intent** - Parse the description to identify:
   - Source system and connection details
   - Destination system and connection details
   - Required transformations or filtering
   - Any special requirements (ordering, batching, etc.)

2. **Discover components** - Find the best match for requirements:
   - Search across relevant component categories
   - Prioritize stable, non-deprecated components
   - Consider configuration complexity vs capabilities

   **When encountering deprecated components**:
   1. Search for modern replacement
   2. Explain benefits of upgrading
   3. Migrate configuration to new component
   4. Preserve original behavior intent

3. **Configure completely** - Build a minimal but complete config:
   - Include all required fields
   - Use reasonable defaults for optional fields when appropriate
   - Structure transformations in the pipeline section
   - Keep it simple - avoid unnecessary complexity

   **When transformations are needed**:
   1. Use `/rpcn:blobl` to create tested scripts
   2. Embed in pipeline.processors section
   3. Ensure scripts handle edge cases (null values, missing fields)

### When Repairing Existing Configurations

1. **Diagnose issues** - Understand what's broken:
   - Run validation to identify errors
   - Consider user-provided context about symptoms
   - Look for common patterns (typos, deprecations, type mismatches)

2. **Explain clearly** - Help user understand the problems:
   - Translate validation errors into plain language
   - Explain why the current configuration doesn't work
   - Identify root causes, not just symptoms

3. **Fix minimally** - Only change what's necessary:
   - Preserve user's original structure and comments
   - Fix errors without unnecessary refactoring
   - Maintain the intent of the original configuration

4. **Verify improvements** - Ensure fixes work:
   - Re-validate after each change
   - Test any modified Bloblang transformations
   - Confirm no regressions introduced

## Quality Standards

- **Handle errors gracefully** - If discovery or validation fails, explain and suggest alternatives
- **Provide context** - Explain component choices and configuration decisions
- **Enable next steps** - Show how to run the config and what to expect
- **Document assumptions** - Note any inferred or default values used

## Output Format

Present results clearly and completely. The final configuration should be written to both the session directory (for validation) and optionally to a user-specified location or displayed for the user to save.

### For Created Configurations
```markdown
## Configuration Created

### pipeline.yaml
[YAML content]

### Validation
✅ Configuration is valid
✅ All required fields provided
✅ No deprecated components

### Environment Variables
Created .env.example with [N] variables:
- KAFKA_PASSWORD - Kafka SASL password
- POSTGRES_DSN - PostgreSQL connection string

### Next Steps
1. Save the configuration to your desired location
2. Copy .env.example to .env and fill in values
3. Add .env to .gitignore
4. Run: rpk connect run pipeline.yaml
```

### For Repaired Configurations
```markdown
## Configuration Repaired: [filename]

### Issues Found: [N]

1. [Error category]
   **Problem**: [Explanation]
   **Fixed**: [What was changed]

### Changes Applied
✅ [Change 1]
✅ [Change 2]

### Validation Results
✅ Configuration now valid
✅ All tests pass

### Optional Improvements
💡 [Suggestion 1]
💡 [Suggestion 2]
```