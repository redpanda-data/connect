---
name: component-search
description: This skill should be used when users need to discover Redpanda Connect components for their streaming pipelines. Trigger when users ask about finding inputs, outputs, processors, or other components, or when they mention specific technologies like "kafka consumer", "postgres output", "http server", or ask "which component should I use for X".
---

# Redpanda Connect Component Search

Help users discover the right Redpanda Connect components for their streaming pipeline needs.

## Objective

Find and recommend the most relevant components that match the user's natural language query.
Provide enough information for users to understand what each component does, how to configure it, and why it matches their needs.

## Prerequisites

This skill requires: `rpk`, `rpk connect`, `python3`.
See the [SETUP](SETUP.md) for installation instructions.

## Component Categories

Redpanda Connect has 8 types of components:
- **inputs** - Read data from sources (Kafka, HTTP, files, databases, etc.)
- **outputs** - Write data to destinations (Kafka, S3, databases, etc.)
- **processors** - Transform, filter, or enrich messages (mapping, filtering, etc.)
- **caches** - Store data for lookups (Redis, in-memory, etc.)
- **rate-limits** - Control throughput (local, Redis-based, etc.)
- **buffers** - Queue messages between pipeline stages
- **metrics** - Export metrics (Prometheus, CloudWatch, etc.)
- **tracers** - Export traces (Jaeger, OTLP, etc.)

## Tools

### Component Discovery

Lists all available components in a category using rpk.

```bash
# Usage:
rpk connect list <category>

# Examples:
rpk connect list inputs
rpk connect list outputs
rpk connect list processors
```
- Categories: inputs, outputs, processors, caches, rate-limits, buffers, metrics, tracers
- Returns list of all component names in that category
- Use this to discover what components exist before searching for specific ones

### Script format-component-fields.sh

Retrieves and formats component configuration schemas.

```bash
# Usage:
./resources/scripts/format-component-fields.sh <category> <component>

# Examples:
./resources/scripts/format-component-fields.sh outputs redis_hash
./resources/scripts/format-component-fields.sh inputs kafka_franz
./resources/scripts/format-component-fields.sh processors mapping
```
- Requires two arguments:
  - category (inputs, outputs, processors, caches, rate-limits, buffers, metrics, tracers)
  - component name (e.g., kafka_franz, redis_hash, postgres)
- Outputs formatted field information grouped by priority:
    - `<required_fields>` - Must be configured
    - `<optional_fields>` - Commonly used settings
    - `<advanced_fields>` - Less common configuration
    - `<secret_fields>` - Sensitive credentials
- Flattens nested fields with dot notation (e.g., `sasl.password`)
- Shows array element types (e.g., `array[string]`)
- Automatically filters deprecated fields

### Script rpk-version.sh

Returns the current Redpanda Connect version in rpk.

```bash
# Usage:
./resources/scripts/rpk-version.sh

# Output example: 4.70.0
```
- No arguments
- Outputs version as a string (e.g., "4.70.0")

### Online Component Documentation

Links to official documentation for detailed component reference.

```
# URL pattern:
https://github.com/redpanda-data/connect/blob/v{version}/docs/modules/components/pages/{category}/{component}.adoc

# Examples:
https://github.com/redpanda-data/connect/blob/v4.70.0/docs/modules/components/pages/inputs/kafka_franz.adoc
https://github.com/redpanda-data/connect/blob/v4.70.0/docs/modules/components/pages/outputs/postgres.adoc
```
- `{version}` - Connect version from rpk-version.sh (e.g., "4.70.0")
- `{category}` - Component category (inputs, outputs, processors, etc.)
- `{component}` - Component name with underscores (e.g., "kafka_franz")

## Workflow

1. **Understand the query**
   - Identify what type of component (input/output/processor/etc.), which technology (kafka/postgres/http), and what action (read/write/transform)
   - If the query is unclear, ask clarifying questions about intent

2. **Find matching components**
   - Discover components across relevant categories that match the user's needs
   - If no exact match exists, recommend similar or related components

3. **Retrieve configuration details**
   - Get schema information for matched components to understand:
     - What fields are required vs optional
     - What the component's capabilities are
     - How complex it is to configure

4. **Rank by relevance**
   - Prioritize components by:
     - How well they match the query intent
     - Their stability status (stable > beta > experimental)
     - Configuration simplicity (fewer required fields) 

5. **Present clearly**
   - Show the top 1-3 results with:
     - Component name and category
     - Brief description of what it does and justification for why it matches the query
     - Configuration requirements (required fields, common optional fields)
     - Minimal configuration example
     - Link to official documentation for more details
     - If component directly matches the query, ignore similar alternatives
