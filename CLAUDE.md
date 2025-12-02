# CLAUDE.md

AI agent guidance for working with Redpanda Connect codebase.

> **Note**: This file contains general repository guidance. For detailed context:
> - **Go patterns & testing**: See `internal/CLAUDE.md`
> - **YAML & Bloblang**: See `config/CLAUDE.md`
> - **Agent standards & gotchas**: See `AGENTS.md`

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

---

## Architecture

### Multi-Distribution System

Four binary distributions exist with different component sets.

**`redpanda-connect`** - Full-featured, self-hosted.
All components (community + enterprise).

**`redpanda-connect-cloud`** - Serverless/cloud environments.
Cloud-safe subset (pure processors only, no filesystem access).

**`redpanda-connect-community`** - Open-source deployments.
FOSS/Apache 2.0 components only.

**`redpanda-connect-ai`** - AI-specific workflows.
Cloud components + AI integrations (OpenAI, Claude, etc.).

Component availability controlled by:
- `public/bundle/enterprise/` and `public/bundle/free/` - Distribution-specific package imports
- `public/schema/` - Schema generation and filtering per distribution
- `internal/plugins/info.csv` - Component metadata defining which components are available in which distributions (columns: `cloud`, `cloud_with_gpu` indicate cloud-safe components)

### Directory Structure

`internal/impl/{category}/` - Component implementations (76+ categories: kafka, redis, aws, azure, postgres, etc.).
Each category contains inputs, outputs, processors, caches for that system.
Components register themselves via `init()` functions calling `service.MustRegister*`.

`public/components/{category}/` - Public API wrappers.
Thin import wrappers: `import _ "github.com/redpanda-data/connect/v4/internal/impl/redis"`.
Allows selective compilation per distribution.

`internal/cli/` - Enterprise CLI functionality (license management, MCP server, agent mode, global manager).

`internal/license/` - RCL (Redpanda Community License) validation and enforcement.

`internal/rpcplugin/` - RPC plugin system for extensibility (Python/Go templates).

`public/schema/` - Distribution-specific schema generation.
`Standard()` for full schema with all components.
`Cloud()` for filtered schema with pure processors only.

`cmd/` - Binary entry points for each distribution.

### Component Registration

Components use registration-at-init via benthos's public service API.
Call `service.MustRegister*` in `init()` function.
Import with `import _` to trigger registration.

No explicit registry file exists.
Components discovered via Go's `import _` side effects.
Different binaries include different subsets by importing different packages.

### Benthos Integration

Benthos is the foundational framework.

Redpanda Connect imports benthos's public service API: `github.com/redpanda-data/benthos/v4/public/service`.
Uses benthos's built-in component interfaces (Input, Output, Processor, Cache, Buffer, etc.).
Inherits benthos's configuration DSL, validation, and runtime.
Extends with enterprise-only components.

Update benthos dependency: `task bump-benthos`

### Other Commands

```bash
task deps                         # Tidy Go modules
task bundles                      # Update bundle imports
```

---

## Additional Context Files

This file provides general repository guidance. For detailed context on specific topics:

- **Go patterns & testing**: See `internal/CLAUDE.md` for Go code patterns, unit testing standards, and integration test patterns
- **YAML & Bloblang**: See `config/CLAUDE.md` for configuration patterns and Bloblang transformation language
- **Agent standards & gotchas**: See `AGENTS.md` for certification requirements and common pitfalls
