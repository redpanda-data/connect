# Kafka Connect → Redpanda Connect Converter — Design

**Date:** 2026-06-22
**Branch:** `kafka_converter`
**Status:** Approved design, pre-implementation

## Purpose

Provide a tool that takes a Kafka Connect connector configuration and produces an
equivalent Redpanda Connect (RPCN) pipeline configuration. This supports
Kafka Connect → Redpanda Connect migration, a real go-to-market path adjacent to
the existing `internal/impl/redpanda/migrator` tooling.

## Form

A **deterministic Go conversion engine packaged as a library, with a thin CLI wrapper.**

- **Deterministic, not LLM-based:** a migration tool must be reproducible and
  testable — same input, same YAML, every time — and must never hallucinate
  component names or config fields. The mapping is fundamentally structured
  (`connector.class` → input/output, converters → codecs/processors, SMTs →
  processors/Bloblang), so it is rule-based work.
- **Library + thin CLI:** the engine is a pure package (bytes in → YAML + report
  out). A thin CLI owns I/O. This keeps the engine reusable from a future cloud
  migration UI or MCP tool at no extra cost now.
- **Graceful degradation:** deterministic means it only converts what it has rules
  for. Unknown connectors/SMTs/converters and leftover fields are surfaced as
  annotated `# TODO` markers in valid YAML — never a hard failure. An LLM/skill
  layer could later sit *on top* of this engine for the long tail; the trustworthy
  core stays deterministic.

## Scope (v1)

**Connectors:**
- Kafka mirror / MirrorMaker (source + destination clusters, checkpoint &
  heartbeat topics) → `kafka_franz` / `redpanda` inputs + outputs
- Snowflake sink → `snowflake_*` output
- BigQuery sink → `gcp_bigquery` output
- Amazon S3 sink → `aws_s3` output
- Google Cloud Storage sink → `gcp_cloud_storage` output
- JDBC source & sink → `sql_select` / `sql_insert` (+ drivers)

Debezium CDC sources are explicitly **out of v1 scope**.

**Cross-cutting:**
- **Converters** (`key.converter` / `value.converter`): common subset —
  Avro → `schema_registry_decode`/`schema_registry_encode`, JSON, String,
  Protobuf. Unknown → annotated TODO.
- **SMTs** (`transforms.*`): a curated set of high-use transforms (e.g.
  InsertField, ReplaceField, RegexRouter) → Bloblang / native processors.
  Unknown → annotated TODO.

**Input formats (v1):** REST-wrapped JSON (`{"name":..., "config":{...}}`) and
flat JSON (bare property map). Both normalize to a flat property map, keeping the
engine input-format-agnostic.
- Out of v1: `.properties` files and live Connect REST API (easy fast-follows
  behind the same engine).

**Output:** RPCN YAML with inline `# TODO:` comments where a field/SMT/connector
could not be fully mapped, plus a short summary printed to stderr.

## Architecture

### Package layout

```
internal/connect_converter/
  convert.go        # Convert(input []byte, opts) (*Result, error)
  parse.go          # REST-wrapped or flat JSON → ConnectConfig{Name, Class, Props}
  registry.go       # connector.class / SMT / converter lookup
  assemble.go       # build RPCN stream yaml.Node tree
  render.go         # serialize with inline # TODO comments
  result.go         # Result{YAML, Warnings}
  connectors/       # mirror, snowflake, bigquery, s3, gcs, jdbc
  smt/              # insertfield, replacefield, regexrouter, ...
  converters/       # avro / json / string / protobuf

internal/cli/convert.go   # thin subcommand: read file/stdin → engine → write YAML
```

The engine takes bytes in, returns YAML + structured warnings. It never touches
the network or process args — the CLI layer owns I/O. Output YAML is built as a
`gopkg.in/yaml.v3` `Node` tree so `# TODO:` comments attach to the exact line they
concern (yaml.v3 is already in the dependency tree via benthos).

### Data flow

```
input bytes
  → parse.go: ConnectConfig{Name, Class, Props}   (unwrap REST "config", normalize types)
  → registry lookup on Class → ConnectorMapper
      → ConnectorMapper.Map(props) → component node (input OR output) + warnings
      → converters: key/value.converter → codec choice or schema_registry_{decode,encode}
      → SMTs: transforms.<name> → []processor nodes (Bloblang or native), in declared order
  → assemble.go: RPCN stream tree
      input:  (sources)   or   output: (sinks)
      pipeline.processors: [ <converter procs>, <SMT procs in order> ]
  → render.go: YAML bytes (+ inline # TODO) , Warnings[] → stderr summary
```

Decisions baked in:
- **Source vs sink** is a property of the connector mapper (it declares which side
  it populates). MirrorMaker → `input` + `output`; sinks → `output`, with `input`
  left as a `# TODO` for the user to point at their source topic.
- **Ordering:** SMT processors are emitted in Kafka Connect's declared `transforms`
  order; converter/deserialization processors come first so SMTs operate on decoded
  data — matching KC runtime semantics.

### Mapper interfaces & registry

Three small interfaces, one per kind of thing being mapped — each an isolated,
independently testable unit.

```go
// A connector.class → an RPCN input or output.
type ConnectorMapper interface {
    Map(cfg ConnectConfig, ctx *MapCtx) (Component, error)
}

// Component declares which side it populates and carries the yaml.Node.
type Component struct {
    Side ComponentSide // Input | Output
    Node *yaml.Node
}

// key/value.converter → deserialization/serialization processors (or a codec hint).
type ConverterMapper interface {
    Map(role ConverterRole, props map[string]any, ctx *MapCtx) ([]*yaml.Node, error)
}

// A single SMT (transforms.<name>) → ordered processor nodes.
type SMTMapper interface {
    Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error)
}
```

`MapCtx` is the shared scratchpad threaded through every mapper:

```go
type MapCtx struct {
    Warn func(field, msg string)   // records a Warning AND attaches an inline # TODO
    // helpers: typed-get from props, mark a field "consumed"
}
```

The **registry** (`registry.go`) holds three maps — `connectors`, `converters`,
`smts` — keyed by Kafka Connect class/alias string, populated via `init()`
registration in each mapper file (same pattern as component registration in this
repo). A lookup miss → a fallback mapper that emits a commented-out stub with
`# TODO: unsupported connector.class=<x>` and a warning, so conversion never
hard-fails.

**Unconsumed-field detection:** mappers mark props they consume; at the end, any
leftover prop becomes a `# TODO: unmapped field <key>=<value>` warning. This makes
"best-effort with honest gaps" automatic rather than per-mapper bookkeeping.

### Output rendering & comment placement

- A mapper flags something via `ctx.Warn(field, msg)`, which both appends a
  structured `Warning` and attaches the message as a `HeadComment`/`LineComment`
  on the relevant node.
- `assemble.go` stitches connector + converter + SMT nodes into the canonical RPCN
  stream shape:
  ```yaml
  input:        # or output: for sinks
    <component>: {...}
  pipeline:
    processors:
      - <converter / SMT processors, in order>
  output:       # for sinks; input: gets a # TODO stub
    <component>: {...}
  ```
- `render.go` marshals the tree (2-space indent) and prints a stderr summary:
  `N warnings: 2 unmapped fields, 1 unsupported SMT…`.
- **Top-of-file provenance header:** a `HeadComment` on the root —
  `# Converted from Kafka Connect connector "<name>" (class=<class>). Review # TODO markers.`

## Error handling

- **Never hard-fail on unmapped content.** Unknown connector/SMT/converter or
  leftover fields → `# TODO` + warning; valid YAML still emitted. Exit code 0,
  warnings on stderr.
- **Hard errors only for genuinely unusable input:** malformed JSON, or missing
  `connector.class`. These return a Go `error` from `Convert`; CLI exits non-zero.
- **No silent drops:** every input property is either mapped or surfaced as a
  warning, guaranteed by the unconsumed-field sweep.

## Testing

- **Per-mapper unit tests** (table-driven): each connector/SMT/converter mapper
  gets focused cases — happy path, unmapped field, type coercion.
- **Golden-file tests:** `testdata/<case>.input.json` → `testdata/<case>.expected.yaml`,
  covering each v1 connector end-to-end including TODO comments.
- **Round-trip validation:** generated YAML is fed through Redpanda Connect's
  existing config linter (`service` lint API) in tests, proving output is valid
  RPCN config, not just well-formed YAML. The `# TODO` stubs are commented, so they
  do not break linting.
- **Parser tests:** REST-wrapped vs flat JSON normalize to the same `ConnectConfig`.

## Out of scope (deferred)

- Debezium CDC source connectors
- `.properties` and live Connect REST API input
- Connectors beyond the v1 list (Elasticsearch, HTTP, etc.)
- A machine-readable (JSON/markdown) report file — v1 uses inline comments +
  stderr summary only
- An LLM/skill layer over the engine for the long tail
