# Redpanda Connect Configuration Plugin

Interactive YAML configuration and Bloblang script authoring for Redpanda Connect with proactive AI assistance.

## Overview

This Claude Code plugin provides intelligent assistance for creating, validating, and fixing Redpanda Connect configurations. It combines semantic component discovery, interactive config generation with security best practices, and tested Bloblang transformations.

## Features

- 🔍 **Semantic Search**: Find components using natural language ("kafka consumer", "postgres output")
- ✨ **Interactive Generation**: Create configs from descriptions with guided validation
- 🔒 **Security First**: Automatic secret detection and environment variable usage
- ✅ **Inline Validation**: Test Bloblang transformations before deployment
- 🤖 **Proactive Assistance**: Agents automatically help when you mention configs or transformations
- 📚 **Version-Matched Docs**: Fetches documentation matching your installed rpk version
- ⚡ **Tested Scripts**: All Bloblang scripts are tested before presenting to user

## Prerequisites

### Required

- **rpk** (>= 23.0.0) - Redpanda CLI with connect support
  ```bash
  # Check version
  rpk version

  # Install on macOS
  brew install redpanda
  ```

- **Python 3** (>= 3.6) - For JSON processing scripts
  ```bash
  python3 --version
  ```

- **jq** - JSON processor (enhances Bloblang testing)
  ```bash
  brew install jq  # macOS
  apt-get install jq  # Ubuntu/Debian
  ```

## Usage

### Three Ways to Get Help

**1. Natural Conversation** (Proactive - Agents auto-trigger)
```
User: "I need to transform JSON data"
→ blobl-helper agent activates
→ Creates tested Bloblang script

User: "How do I read from Kafka?"
→ component-finder agent activates
→ Searches for Kafka input components

User: "Create a pipeline from S3 to Postgres"
→ config-assistant agent activates
→ Generates complete validated config
```

**2. Explicit Commands** (User control)
```bash
/rpcn:search kafka consumer
/rpcn:blobl "convert timestamp to ISO format"
/rpcn:pipeline "read from Kafka to stdout"
```

**3. Claude Auto-loads Skills** (When context matches)
Claude automatically loads relevant skills based on conversation context without explicit commands.

## Commands

### `/rpcn:search <query>`

Semantic search for Redpanda Connect components.

**Examples:**
```bash
/rpcn:search kafka consumer
/rpcn:search postgres output
/rpcn:search http server
/rpcn:search redis cache
```

**Returns:**
- Top 3-5 most relevant components
- Configuration requirements (required/optional fields)
- Minimal usage examples
- Documentation links

### `/rpcn:blobl <description> [sample_input]`

Create and test Bloblang transformation scripts.

**Examples:**
```bash
/rpcn:blobl "convert timestamp to ISO format"
/rpcn:blobl "uppercase name field and add UUID"
/rpcn:blobl "filter array elements where age > 18"
/rpcn:blobl "parse JSON and extract nested field" '{"user": {"name": "test"}}'
```

**Process:**
1. Analyzes transformation requirements
2. Discovers relevant Bloblang functions/methods
3. Generates script with proper syntax
4. Tests against sample input (provided or generated)
5. Iterates until script works correctly
6. Returns working script with examples

**Quality guarantees:**
- ✅ Never presents untested code
- ✅ Uses real Bloblang functions (not guessed)
- ✅ Includes tested input/output pairs
- ✅ Handles edge cases (null values, missing fields)

### `/rpcn:pipeline <target> [context]`

Create or repair Redpanda Connect configurations.

**Creating new configs:**
```bash
/rpcn:pipeline "Read from Kafka on localhost:9092 topic 'events' to stdout"
/rpcn:pipeline "Stream from S3 bucket to Postgres with transformations"
/rpcn:pipeline "HTTP server that writes to Redis"
```

**Repairing existing configs:**
```bash
/rpcn:pipeline config.yaml
/rpcn:pipeline pipeline.yaml "getting connection timeout"
/rpcn:pipeline broken.yaml "deprecated component"
```

**Process for creation:**
1. Parses description to identify input, output, transformations
2. Uses `/rpcn:search` to discover components
3. Builds complete YAML configuration
4. Uses environment variables for all secrets
5. Validates with `validate-pipeline.sh`
6. Creates `.env.example` file

**Process for repair:**
1. Reads configuration file
2. Runs validation to identify errors
3. Explains problems in plain language
4. Applies minimal fixes
5. Re-validates after changes

**Security:**
- ✅ All passwords, secrets, tokens use environment variables
- ✅ Never stores credentials in YAML
- ✅ Creates `.env.example` with descriptions
- ✅ Reminds user to add `.env` to `.gitignore`
