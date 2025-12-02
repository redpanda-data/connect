# Redpanda Connect Configuration Plugin

Interactive YAML configuration and Bloblang script authoring for Redpanda Connect.

## Overview

This Claude Code plugin provides intelligent assistance for creating, validating, and fixing Redpanda Connect configurations. It features semantic component discovery, interactive config generation with security best practices, and smart error fixing.

## Features

- 🔍 **Semantic Search**: Find components, functions, and methods using natural language
- ✨ **Interactive Generation**: Create configs from descriptions with guided prompts
- 🔒 **Security First**: Automatic secret detection and environment variable usage
- ✅ **Inline Validation**: Test Bloblang transformations and validate configs
- 🔧 **Smart Fixing**: Intelligent error fixing with deprecation handling
- 📚 **Version-Matched Docs**: Fetches documentation matching your installed version
- ⚡ **Context Efficient**: Uses Python scripts for optimal JSON processing

## Prerequisites

### Required

- **rpk** (>= 23.0.0) - Redpanda CLI with connect support
  ```bash
  # Check version
  rpk version
  ```

- **jq** - JSON processor (required for Bloblang testing)
  ```bash
  # Install on macOS
  brew install jq

  # Install on Ubuntu/Debian
  apt-get install jq

  # Check version
  jq --version
  ```

- **Python 3** (>= 3.6) - For JSON processing scripts
  ```bash
  # Check version
  python3 --version
  ```

### Optional

- **gh** - GitHub CLI for faster documentation fetching
  ```bash
  brew install gh
  gh auth login
  ```

## Installation

### From Source

```bash
cd /path/to/connect/.claude-plugin
# Plugin is auto-discovered by Claude Code
```

### From Marketplace (Future)

```bash
/plugin marketplace add redpanda-data/connect-config-plugin
```

## Commands

### `/rpcn:search <query>`

Semantic search for components, functions, and methods.

**Examples:**
```bash
/rpcn:search kafka consumer
/rpcn:search transform json
/rpcn:search postgres output
/rpcn:search uuid function
```

**How it works:**
1. Searches across all component categories, bloblang functions, and methods
2. Fetches schemas for matches and filters by relevance
3. Retrieves detailed documentation for top 3-5 candidates
4. Returns recommendations with justification

### `/rpcn:new <description>`

Generate a new configuration from natural language description.

**Examples:**
```bash
/rpcn:new "Read from Kafka localhost:9092 topic 'events' to stdout"
/rpcn:new "HTTP API to Postgres with field transformations"
/rpcn:new "Transform JSON messages and write to S3"
```

**How it works:**
1. Analyzes your description to identify required components
2. Uses `/rpcn:search` internally to find best matches
3. Prompts interactively for ALL required fields
4. Detects sensitive fields (passwords, secrets, tokens)
5. Uses environment variables for secrets
6. Generates complete YAML with validation
7. Tests Bloblang transformations inline
8. Creates .env.example if needed

**Security:**
- ✅ Passwords NEVER stored in YAML
- ✅ Passwords NEVER put in Claude context
- ✅ Environment variable references used: `${KAFKA_PASSWORD}`
- ✅ .env.example file created with descriptions

### `/rpcn:fix <file> [context]`

Fix an existing configuration with validation and suggestions.

**Examples:**
```bash
/rpcn:fix config.yaml
/rpcn:fix pipeline.yaml "getting connection timeout"
/rpcn:fix broken.yaml "deprecated kafka_balanced component"
```

**How it works:**
1. Reads existing configuration
2. Runs `rpk connect lint` to identify errors
3. Explains errors in plain language
4. Fetches updated docs for deprecated components
5. Suggests or applies fixes
6. Re-validates after changes
7. Handles: missing fields, type errors, deprecations, Bloblang bugs

## Usage Examples

### Example 1: Discover Kafka Components

```bash
/rpcn:search kafka consumer
```

**Output:**
```
Found 3 relevant components:

1. kafka_franz (input)
   - Modern Kafka consumer using franz-go library
   - Supports: consumer groups, headers, TLS, SASL
   - Status: stable
   - Example: ...

2. kafka (input) [DEPRECATED]
   - Legacy Kafka consumer
   - Consider using kafka_franz instead
   ...
```

### Example 2: Generate Simple Pipeline

```bash
/rpcn:new "Read from Kafka localhost:9092 topic 'test' to stdout"
```

**Interactive prompts:**
```
Required fields for kafka_franz input:
  [1/3] brokers (array): ["localhost:9092"]
  [2/3] topics (array): ["test"]
  [3/3] consumer_group (string): my-group

Generating configuration...
```

**Generated config.yaml:**
```yaml
input:
  kafka_franz:
    brokers:
      - localhost:9092
    topics:
      - test
    consumer_group: my-group

output:
  stdout: {}
```

**Validation:**
```
✅ Configuration is valid
✅ No deprecated components
Ready to use!
```

### Example 3: Fix Broken Config

```bash
/rpcn:fix broken.yaml
```

**Output:**
```
Found 2 errors:

1. Line 5: Field 'broker' not recognized
   → Did you mean 'brokers' (array)?

2. Line 12: Missing required field 'consumer_group'
   → This field is required for kafka_franz

Apply fixes? (y/n)
```

## Architecture

### Directory Structure

```
.claude-plugin/
├── plugin.json           # Plugin manifest
├── marketplace.json      # Marketplace metadata
├── README.md            # This file
├── skills/
│   └── rpcn-config.md   # Main knowledge base
├── agents/
│   ├── config-writer.md # YAML generation
│   ├── bloblang-expert.md # Bloblang transformations
│   ├── validator.md     # Validation & fixing
│   └── doc-fetcher.md   # Doc retrieval
├── commands/
│   ├── search.md        # /rpcn:search command
│   ├── new.md           # /rpcn:new command
│   └── fix.md           # /rpcn:fix command
└── cache/
    └── <version>/       # Cached documentation
```

### Design Principles

1. **Command-Driven**: User controls workflow via slash commands
2. **Specialist Agents**: Agents handle complex sub-tasks
3. **Security First**: Secrets always in environment variables
4. **Context Efficient**: JSON processed with Python scripts, not raw
5. **Version Aware**: Docs matched to installed rpk version

## Troubleshooting

### rpk command not found

```bash
# Install Redpanda
brew install redpanda

# Or download from
# https://github.com/redpanda-data/redpanda/releases
```

### python3 command not found

```bash
# macOS (usually pre-installed)
brew install python3

# Ubuntu/Debian
apt-get install python3

# Check version
python3 --version
```

### Bloblang metadata not available

If you get component schemas instead of bloblang metadata:

```bash
# Check rpk connect version
rpk connect --version

# Should be >= 4.72.0 with metadata support
```

### Cache issues

```bash
# Clear documentation cache
rm -rf .claude-plugin/cache/*

# Docs will be re-fetched on next use
```

## Development

### Testing Commands

```bash
# Test search
/rpcn:search kafka

# Test new config generation
/rpcn:new "simple test pipeline"

# Test fix with sample config
/rpcn:fix config/example.yaml
```

### Adding New Components

The plugin automatically discovers new components via `rpk connect list`.
No manual updates needed when Redpanda Connect adds components.

## Resources

- [Redpanda Connect Documentation](https://docs.redpanda.com/redpanda-connect)
- [Bloblang Language Reference](https://docs.redpanda.com/redpanda-connect/bloblang/about)
- [Component Reference](https://docs.redpanda.com/redpanda-connect/components/about)
- [GitHub Repository](https://github.com/redpanda-data/connect)

## Support

- 🐛 Report issues: https://github.com/redpanda-data/connect/issues
- 💬 Community: https://redpanda.com/slack
- 📖 Docs: https://docs.redpanda.com

## License

Apache 2.0 - See LICENSE file for details
