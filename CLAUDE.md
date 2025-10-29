# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Redpanda Connect is a high-performance stream processor built on top of **benthos** (github.com/redpanda-data/benthos/v4). This repository adds enterprise features, proprietary connectors, and Redpanda-specific optimizations to the upstream benthos framework.

## Build Commands

### Building
```bash
task build:all                    # Build all 4 binary distributions
task build:redpanda-connect       # Build full-featured binary
task build:redpanda-connect-cloud # Build cloud-safe version (no filesystem)
task build:redpanda-connect-community # Build Apache 2.0 only version
task build:redpanda-connect-ai    # Build AI-focused version

# Build with external dependencies (ZMQ, etc.)
TAGS=x_benthos_extra task build:all
```

### Testing
```bash
task test                         # Run unit and template tests
task test:unit                    # Run unit tests only
task test:unit-race               # Run unit tests with race detection
task test:template                # Run template/Bloblang tests
task test:integration-package PKG=./internal/impl/kafka/...  # Run integration tests for specific package
```

Integration tests require Docker and are skipped by default. Run them individually per component.

### Code Quality
```bash
task fmt                          # Format code with gofumpt
task lint                         # Run golangci-lint
task vuln                         # Run vulnerability scanner
```

### Running Locally
```bash
task run                          # Run with default config (config/dev.yaml)
task run CONF=./path/to/config.yaml # Run with specific config

# Or directly with go
go run ./cmd/redpanda-connect --config ./config.yaml
```

## Architecture

### Multi-Distribution System

Four binary distributions exist with different component sets:

| Binary | Purpose | Components |
|--------|---------|------------|
| `redpanda-connect` | Full-featured, self-hosted | All (community + enterprise) |
| `redpanda-connect-cloud` | Serverless/cloud environments | Cloud-safe subset (pure processors only, no filesystem access) |
| `redpanda-connect-community` | Open-source deployments | FOSS/Apache 2.0 components only |
| `redpanda-connect-ai` | AI-specific workflows | Cloud components + AI integrations (OpenAI, Claude, etc.) |

Component availability is controlled by:
- `public/bundle/` - Distribution-specific component imports
- `public/schema/` - Schema generation per distribution (gating components)
- `internal/plugins/info.csv` - Component metadata (community/enterprise classification)

### Directory Structure

- **`internal/impl/{category}/`** - Component implementations (76+ categories: kafka, redis, aws, azure, postgres, etc.)
  - Each category contains inputs, outputs, processors, caches for that system
  - Components register themselves via `init()` functions calling `service.MustRegister*`

- **`public/components/{category}/`** - Public API wrappers
  - Thin import wrappers: `import _ "github.com/redpanda-data/connect/v4/internal/impl/redis"`
  - Allows selective compilation per distribution

- **`internal/cli/`** - Enterprise CLI functionality
  - License management, MCP server, agent mode, global manager

- **`internal/license/`** - RCL (Redpanda Community License) validation and enforcement

- **`internal/rpcplugin/`** - RPC plugin system for extensibility (Python/Go templates)

- **`public/schema/`** - Distribution-specific schema generation
  - `Standard()` - Full schema with all components
  - `Cloud()` - Filtered schema with pure processors only

- **`cmd/`** - Binary entry points for each distribution

### Component Registration Pattern

Components use **registration-at-init** via benthos's public service API:

```go
// internal/impl/redis/cache.go
func init() {
    service.MustRegisterCache("redis", redisCacheConfig(),
        func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
            return newRedisCacheFromConfig(conf)
        })
}

// public/components/redis/package.go
import _ "github.com/redpanda-data/connect/v4/internal/impl/redis"
```

**Key insight:** There's no explicit registry file. Components are discovered via Go's `import _` side effects. This allows different binaries to include different subsets of components simply by importing different packages.

### Benthos Integration

Benthos is the foundational framework. Redpanda Connect:
- Imports benthos's public service API: `github.com/redpanda-data/benthos/v4/public/service`
- Uses benthos's built-in component interfaces (Input, Output, Processor, Cache, Buffer, etc.)
- Inherits benthos's configuration DSL, validation, and runtime
- Extends with enterprise-only components

**Update benthos dependency:** `task bump-benthos`

## Development Workflow

### Adding a New Component

To add a new input connector (e.g., "foo"):

1. **Create implementation:** `internal/impl/foo/input.go`
   ```go
   func init() {
       service.MustRegisterInput("foo", fooInputConfig(), newFooInput)
   }
   ```

2. **Add public wrapper:** `public/components/foo/package.go`
   ```go
   import _ "github.com/redpanda-data/connect/v4/internal/impl/foo"
   ```

3. **Add license header:**
   - Apache 2.0 = community component (available in all distributions)
   - RCL = enterprise component (only in full `redpanda-connect` binary)

4. **Update metadata (if enterprise-only):** `internal/plugins/info.csv`

5. **Add tests:**
   - Unit tests: `internal/impl/foo/input_test.go`
   - Integration tests: `internal/impl/foo/input_integration_test.go`
   - Use `ory/dockertest` for containerized dependencies

6. **Verify:** `task fmt && task lint && task test`

### Running a Single Test

```bash
# Unit test
go test -v -run TestFooInput ./internal/impl/foo/

# Integration test (requires Docker)
go test -v -run "^Test.*Integration.*$" ./internal/impl/foo/
```

## Certification Standards (from CONTRIBUTING.md)

Certified connectors must have:
- **Documentation:** Examples, troubleshooting, known limitations documented
- **Observability:** Metrics, logs (warnings/errors only during issues), tracing hooks
- **Testing:** Integration tests with containerized dependencies runnable in CI
- **Code quality:** Idiomatic Go, consistent with existing patterns, follows Effective Go
- **UX validation:** Strong config linting with clear error messages
- **Credential rotation:** Support live credential updates without downtime (where applicable)

Anti-patterns to avoid:
- Incomplete implementations
- Unfamiliar or confusing UX patterns inconsistent with other connectors
- Excessive resource usage (unnecessary goroutines, memory/CPU overhead)
- Hard-to-diagnose error handling

## License Headers

CI enforces license headers on all files:
- **Apache 2.0:** Community components (included in all distributions)
- **RCL (Redpanda Community License):** Enterprise-only components

The license header determines distribution availability. Check `internal/license/` for validation logic.

## Key Non-Obvious Patterns

1. **Initialization via `init()`:** All component registration happens in `init()` functions. No central registry file exists. Components are included via `import _` side effects.

2. **Config spec is self-documenting:** `service.ConfigSpec` objects serve triple duty as schema + validation + documentation. They generate CLI help, web UI docs, and JSON schema.

3. **Distribution gating happens at compile time:** Different binaries import different `public/components/` packages. The schema filters components at runtime based on `internal/plugins/info.csv`.

4. **Template tests validate YAML configs:** `task test:template` runs actual binary against config files in `config/test/` and `internal/impl/*/tmpl.yaml` to ensure examples work.

5. **Integration tests use Docker:** Integration tests named `*_integration_test.go` use `ory/dockertest` to spin up real dependencies. They're skipped by `task test` but run individually via `task test:integration-package`.

## Common Gotchas

- **External dependencies:** By default, components requiring external C libraries (like ZMQ) are excluded. Use `TAGS=x_benthos_extra task build:all` to include them.
- **Template tests can be slow:** They build and run actual binaries. Run only changed tests during development.
- **Cloud distribution is restrictive:** Only pure processors (no side effects) and pure Bloblang functions are allowed. Check `schema.Cloud()` for filtering logic.
- **License headers matter:** CI will fail if headers don't match the component's distribution classification.
