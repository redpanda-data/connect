# AGENTS.md - Agent Behavior Standards

Standards, certification requirements, and common gotchas for AI agents working with Redpanda Connect.

---

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

---

## License Headers

CI enforces license headers on all files:
- **Apache 2.0:** Community components (included in all distributions)
- **RCL (Redpanda Community License):** Enterprise-only components

The license header determines distribution availability. Check `internal/license/` for validation logic.

---

## Key Non-Obvious Patterns

1. **Initialization via `init()`:** All component registration happens in `init()` functions. No central registry file exists. Components are included via `import _` side effects.

2. **Config spec is self-documenting:** `service.ConfigSpec` objects serve triple duty as schema + validation + documentation. They generate CLI help, web UI docs, and JSON schema.

3. **Distribution gating happens at compile time:** Different binaries import different `public/components/` packages. The schema filters components at runtime based on `internal/plugins/info.csv`.

4. **Template tests validate YAML configs:** `task test:template` runs actual binary against config files in `config/test/` and `internal/impl/*/tmpl.yaml` to ensure examples work.

5. **Integration tests use Docker:** Integration tests named `*_integration_test.go` use `ory/dockertest` to spin up real dependencies. They're skipped by `task test` but run individually via `task test:integration-package`

---

## Common Gotchas

- **External dependencies:** By default, components requiring external C libraries (like ZMQ) are excluded. Use `TAGS=x_benthos_extra task build:all` to include them.
- **Template tests can be slow:** They build and run actual binaries. Run only changed tests during development.
- **Cloud distribution is restrictive:** Only pure processors (no side effects) and pure Bloblang functions are allowed. Check `schema.Cloud()` for filtering logic.
- **License headers matter:** CI will fail if headers don't match the component's distribution classification.
- **Bloblang input must end with newline:** When testing with `blobl`, ensure input JSON ends with `\n`.
- **Integration tests require Docker:** They use `ory/dockertest` to spin up real dependencies. Check `integration.CheckSkip(t)` at the start of each test.
- **Component registration is implicit:** Components register via `init()` functions. Importing a package with `import _` triggers registration.
