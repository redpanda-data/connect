Use this skill when users need to discover Redpanda Connect components for their streaming pipelines. Trigger when users ask about finding inputs, outputs, processors, or other components, or when they mention specific technologies like "kafka consumer", "postgres output", "http server", or ask "which component should I use for X".

---

# Redpanda Connect Component Search

Help users discover the right Redpanda Connect components for their streaming pipeline needs.

## Objective

Find and recommend the 3-5 most relevant components that match the user's natural language query. Provide enough information for users to understand what each component does, how to configure it, and why it matches their needs.

## Context

**Component Categories:**

Redpanda Connect has 8 types of components:
- **inputs** - Read data from sources (Kafka, HTTP, files, databases, etc.)
- **outputs** - Write data to destinations (Kafka, S3, databases, etc.)
- **processors** - Transform, filter, or enrich messages (mapping, filtering, etc.)
- **caches** - Store data for lookups (Redis, in-memory, etc.)
- **rate-limits** - Control throughput (local, Redis-based, etc.)
- **buffers** - Queue messages between pipeline stages
- **metrics** - Export metrics (Prometheus, CloudWatch, etc.)
- **tracers** - Export traces (Jaeger, OTLP, etc.)

**Component Discovery:**

Use `rpk connect list <category>` to list all available components in a category (e.g., `rpk connect list inputs`).

**Available Resources:**

**format-component-fields.sh** - Retrieves and formats component configuration schemas
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh <category> <component>

# Examples:
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh input kafka_franz
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh output postgres
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh processor mapping
```
- Takes two parameters: category (singular) and component name
- Categories: input, output, processor, cache, rate-limit, buffer, metric, tracer
- Returns structured field information with priority groupings
- Output format uses XML tags to separate field types

**rpk-version.sh** - Returns the current rpk/Connect version
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/rpk-version.sh

# Output example: 4.70.0
```
- Returns version string for documentation URL construction
- No parameters required

## Requirements

1. **Understand the query** - Identify what type of component the user needs (input/output/processor/etc.), which technology (kafka/postgres/http), and what action (read/write/transform)

2. **Find matching components** - Discover components across relevant categories that match the user's needs

3. **Retrieve configuration details** - Get schema information for matched components to understand:
   - What fields are required vs optional
   - What the component's capabilities are
   - How complex it is to configure

4. **Rank by relevance** - Prioritize components by:
   - How well they match the query intent
   - Their stability status (stable > beta > experimental)
   - Configuration simplicity (fewer required fields)
   - Whether they're deprecated (avoid unless specifically requested)

5. **Present clearly** - Show the top 3-5 results with:
   - Component name and category
   - Brief description of what it does
   - Configuration requirements (required fields, common optional fields)
   - Minimal usage example
   - Justification for why it matches the query

## Tool Interactions

**format-component-fields.sh**:
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh <category> <component>

# Categories (singular): input, output, processor, cache, rate-limit, buffer, metric, tracer
# Examples:
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh input kafka_franz
${CLAUDE_PLUGIN_ROOT}/scripts/format-component-fields.sh processor mapping
```
- Accepts two positional parameters: category (singular) and component name
- Returns formatted field information grouped by priority:
  - `<required_fields>` - Must be configured
  - `<optional_fields>` - Commonly used settings
  - `<advanced_fields>` - Less common configuration
  - `<secret_fields>` - Sensitive credentials
- Flattens nested fields with dot notation (e.g., `sasl.password`)
- Shows array element types (e.g., `array[string]`)
- Automatically filters deprecated fields

**rpk-version.sh**:
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/rpk-version.sh

# Output: version string (e.g., "4.9.2")
```
- Returns the version string for documentation URL construction
- No parameters required
- Documentation location pattern: `https://github.com/redpanda-data/connect/blob/v{version}/docs/modules/components/pages/{category}/{component}.adoc`

## Quality Standards

- **Prioritize relevance** - Show only the most relevant 3-5 components, not exhaustive lists
- **Explain matches** - Justify why each component matches the query
- **Provide examples** - Include minimal working configuration examples
- **Handle ambiguity** - If the query is unclear, ask clarifying questions about intent
- **Suggest alternatives** - If no exact match exists, recommend similar or related components
