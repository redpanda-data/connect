# CLAUDE.md

AI agent guidance for working with Redpanda Connect codebase.

---

## Agent Routing

| Task | Agent/File |
|---|---|
| Writing or modifying Go code | `godev` agent |
| Writing or modifying tests | `tester` agent |
| Code review | `/review` command |

## Plugin: Redpanda Connect

YAML configuration, Bloblang authoring, and component discovery are provided by the `redpanda-connect` plugin from `.claude-plugin`.

### Prerequisites

```bash
brew install redpanda-data/tap/redpanda python3 jq
rpk connect install
```

### Installation

```bash
/plugin marketplace add /path/to/connect   # local dev
/plugin install redpanda-connect
```

Restart Claude Code after installation.

### Commands

| Command | Purpose |
|---|---|
| `/rpcn:search <query>` | Natural language component discovery |
| `/rpcn:blobl <description> [sample=<json>]` | Bloblang transformation authoring |
| `/rpcn:pipeline <description> [file=<path>]` | Pipeline creation and repair |

The plugin also auto-triggers on mentions of Redpanda Connect, streaming pipelines, or Bloblang.

---

## Project Overview

Redpanda Connect is a high-performance stream processor built on **benthos** (`github.com/redpanda-data/benthos/v4`).
This repository adds enterprise features, proprietary connectors, and Redpanda-specific optimizations to the upstream benthos framework.

---

## Build Commands

### Building
```bash
task build:all                    # Build all 4 binary distributions
task build:redpanda-connect       # Full-featured binary
task build:redpanda-connect-cloud # Cloud-safe version (no filesystem)
task build:redpanda-connect-community # Apache 2.0 only version
task build:redpanda-connect-ai    # AI-focused version

# Build with external dependencies (ZMQ, etc.)
TAGS=x_benthos_extra task build:all
```

### Testing
```bash
task test                         # Run unit and template tests
task test:unit                    # Run unit tests only (alias: task test:ut)
task test:unit-race               # Run unit tests with race detection
task test:template                # Run template/Bloblang tests (alias: task test:tmpl)
task test:integration-package PKG=./internal/impl/kafka/...  # Run integration tests (alias: task test:it PKG=...)

# Run specific test
go test -v -run TestFunctionName ./internal/impl/category/

# Run integration test for specific package (requires Docker)
go test -v -run "^Test.*Integration.*$" ./internal/impl/kafka/
```

Integration tests require Docker and are skipped by default.
Run them individually per component.

### Code Quality
```bash
task fmt                          # Format code with gofumpt
task lint                         # Run golangci-lint
task vuln                         # Run vulnerability scanner
task build:clean                  # Clean build artifacts
```

### Documentation
```bash
task docs                         # Generate documentation and validate examples
```

### Running Locally
```bash
task run                          # Run with default config (config/dev.yaml)
task run CONF=./path/to/config.yaml # Run with specific config

# Or directly with go
go run ./cmd/redpanda-connect --config ./config.yaml

# Or using rpk (if installed)
rpk connect run ./config.yaml
```

### Other Commands
```bash
task deps                         # Tidy Go modules
task bundles                      # Update bundle imports
task bump-benthos                 # Update benthos dependency
```

---

## Architecture

### Multi-Distribution System

Four binary distributions with different component sets:

| Distribution | Purpose | Components |
|---|---|---|
| `redpanda-connect` | Full-featured, self-hosted | All (community + enterprise) |
| `redpanda-connect-cloud` | Serverless/cloud | Cloud-safe subset, no filesystem |
| `redpanda-connect-community` | Open-source | Apache 2.0 only |
| `redpanda-connect-ai` | AI workflows | Cloud + AI integrations |

Component availability controlled by:
- `public/bundle/enterprise/` and `public/bundle/free/` - Distribution-specific package imports
- `public/schema/` - Schema generation and filtering per distribution
- `internal/plugins/info.csv` - Component metadata (columns: `cloud`, `cloud_with_gpu`)

### Directory Structure

`internal/impl/{category}/` - Component implementations. Each category contains inputs, outputs, processors, caches for that system.

`public/components/{category}/` - Public API wrappers. Thin `import _` wrappers for selective compilation.

`internal/cli/` - Enterprise CLI (license management, MCP server, agent mode).

`internal/license/` - RCL validation and enforcement.

`internal/rpcplugin/` - RPC plugin system (Python/Go templates).

`public/schema/` - Distribution-specific schema generation.

`cmd/` - Binary entry points for each distribution.

### Benthos Integration

Redpanda Connect imports benthos's public service API: `github.com/redpanda-data/benthos/v4/public/service`.
Inherits benthos's component interfaces, configuration DSL, validation, and runtime.

Component registration, config specs, license headers, and certification standards are covered in the `godev` agent.

---

## Key Non-Obvious Patterns

1. **Distribution gating is compile-time:** Different binaries import different `public/components/` packages. Schema filters at runtime based on `internal/plugins/info.csv`.

2. **Template tests validate YAML configs:** `task test:template` runs actual binaries against config files in `config/test/` and `internal/impl/*/tmpl.yaml`.

3. **Cloud distribution is restrictive:** Only pure processors (no side effects) and pure Bloblang functions. Check `schema.Cloud()` for filtering logic.

---

## Common Gotchas

- **External dependencies:** Components requiring C libraries (like ZMQ) are excluded by default. Use `TAGS=x_benthos_extra task build:all`.
- **Template tests are slow:** They build and run actual binaries. Run only changed tests during development.
- **License headers matter:** CI fails if headers don't match the component's distribution classification. See `godev` agent for header formats.
