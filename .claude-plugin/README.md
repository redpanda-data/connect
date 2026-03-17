# Redpanda Connect Plugin

AI-powered assistant for building Redpanda Connect streaming pipelines with natural language.

**What you get:**
- Component discovery using natural language
- Pipeline generation from descriptions
- Bloblang transformation authoring
- Configuration validation and fixing

## Use in Claude Code

### Prerequisites

```bash
# Install Redpanda rpk CLI tool
brew install redpanda-data/tap/redpanda

# Install or upgrade Redpanda Connect
rpk connect install
rpk connect upgrade

# Install Python and jq (required by plugin)
brew install python3 jq

# Verify installation
rpk version        
python3 --version  
jq --version
```

### Plugin Installation

**From GitHub (recommended):**

```bash
# Add marketplace
/plugin marketplace add https://github.com/redpanda-data/connect.git

# Install plugin
/plugin install redpanda-connect
```

**Local development:**

```bash
# Add local marketplace
/plugin marketplace add /path/to/connect

# Install plugin
/plugin install redpanda-connect
```

Restart Claude Code after installation.

### Quick Start

Three slash commands provide direct access:

- `/rpcn:search` - Natural language component discovery
- `/rpcn:blobl` - Bloblang transformation script generation
- `/rpcn:pipeline` - End-to-end pipeline orchestration

Claude will also automatically assist when you mention Redpanda Connect, streaming pipelines, or Bloblang in conversation.

### Commands Reference

#### `/rpcn:search <query>`

Search for components using natural language.

**Examples:**

```bash
/rpcn:search "kafka consumer"
/rpcn:search "postgres output with connection pooling"
/rpcn:search "rate limiting"
```

#### `/rpcn:blobl <description> [sample=<json>]`

Generate tested Bloblang transformation scripts.

**Examples:**

```bash
# Basic transformation
/rpcn:blobl "parse JSON and extract user.name field"

# With test data
/rpcn:blobl "uppercase name" sample='{"name": "john"}'
```

#### `/rpcn:pipeline <description> [file=<path>]`

Create new pipelines or fix existing configurations.

**Examples: Create new pipeline:**

```bash
/rpcn:pipeline "consume from Kafka, transform with Bloblang, output to S3"
/rpcn:pipeline "HTTP webhook receiver that writes to PostgreSQL"
```

**Examples: Fix existing pipeline:**

```bash
/rpcn:pipeline "fix connection timeout" file=config.yaml
/rpcn:pipeline "add retry logic" file=pipeline.yaml
```

---

## Use in Claude Desktop

If you're using Claude Desktop (not Claude Code), you can manually install individual skills as standalone tools.

### Skills

- `component-search`: Natural language component discovery
- `bloblang-authoring`: Bloblang transformation script generation
- `pipeline-assistant`: End-to-end pipeline orchestration

### Installation

Three skills are available as ZIP files in `./dist/` directory.
Drag the ZIP files individually into Claude Desktop Settings > Capabilities to install.

### Usage

Once installed the skills will automatically assist when you mention Redpanda Connect, streaming pipelines, or Bloblang in conversation.
You may also trigger them explicitly using keywords like `component-search skill`, `bloblang-authoring skill`, or `pipeline-assistant skill`.
