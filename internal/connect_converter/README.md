# Kafka Connect → Redpanda Connect converter

`connectconverter` turns a [Kafka Connect](https://docs.confluent.io/platform/current/connect/index.html)
connector configuration into an equivalent [Redpanda Connect](https://docs.redpanda.com/redpanda-connect/)
(RPCN) pipeline YAML. It is a deterministic, rule-based engine — the same input
always produces the same output — exposed as a library and a thin `convert` CLI
subcommand.

## Philosophy

The converter is **best-effort and never destructive**:

- It maps what it has explicit rules for and annotates everything else with inline
  `# TODO:` comments instead of guessing or failing.
- Every Kafka Connect property is either mapped or surfaced as a warning — nothing
  is silently dropped.
- The only hard errors are genuinely unusable input: malformed JSON, or a missing
  `connector.class`. Unknown connectors, converters, SMTs, and stray fields all
  degrade to valid YAML with TODO markers and a warning count on stderr.

Because the output is meant to be reviewed and finished by a human, treat the
generated YAML as a strong starting point, not a drop-in replacement.

## Usage

The engine is wired into the binary as a `convert` subcommand:

```bash
# From a file, write YAML to a file
rpk connect convert ./s3-sink.json -o ./s3-sink.yaml

# From stdin to stdout
cat connector.json | rpk connect convert
```

It writes the YAML to `-o`/`--output` (or stdout), prints a one-line warning
summary to stderr, and exits `0` even when there are TODO markers — non-zero only
on unusable input.

### Web playground

For interactive use, `convert server` starts a local two-pane playground — paste a
Kafka Connect config on the left, see the live Redpanda Connect YAML (with `# TODO`
markers and a warning count) on the right:

```bash
rpk connect convert server                 # http://localhost:4196
rpk connect convert server --http :8080    # custom bind address
```

It serves a single self-contained page and a `POST /convert` endpoint that calls
the same engine; the UI updates as you type. Localhost-bound; intended as a local
dev tool. Implementation lives in `internal/cli/convertserver`.

### Getting the Kafka Connect JSON

The converter accepts JSON only (it never talks to the network itself). Fetch the
config from a running Connect cluster's REST API (default port `8083`):

```bash
# Full connector definition — REST-wrapped form {"name":..., "config":{...}}
curl -s http://localhost:8083/connectors/s3-sink | rpk connect convert

# Just the config map — flat form
curl -s http://localhost:8083/connectors/s3-sink/config -o s3-sink.json
rpk connect convert s3-sink.json -o s3-sink.yaml

# Convert every connector in a cluster
for c in $(curl -s http://localhost:8083/connectors | jq -r '.[]'); do
  curl -s "http://localhost:8083/connectors/$c" | rpk connect convert -o "$c.yaml"
done
```

Both the REST-wrapped shape (`{"name":..., "config":{...}}`) and the flat property
map are accepted and normalized to the same internal representation.

### Library

```go
res, err := connectconverter.Convert(inputJSON)
if err != nil {
    // malformed JSON or missing connector.class
}
fmt.Print(string(res.YAML))
for _, w := range res.Warnings {
    log.Printf("%s: %s", w.Field, w.Message)
}
```

## What's supported (v1)

**Input formats:** REST-wrapped JSON, flat JSON.

**Connectors**

| Kafka Connect class | RPCN component |
|---|---|
| `io.confluent.connect.s3.S3SinkConnector` | `aws_s3` output |
| `io.confluent.connect.gcs.GcsSinkConnector` | `gcp_cloud_storage` output |
| `com.wepay.kafka.connect.bigquery.BigQuerySinkConnector` | `gcp_bigquery` output |
| `com.snowflake.kafka.connector.SnowflakeSinkConnector` | `snowflake_streaming` output |
| `io.confluent.connect.jdbc.JdbcSourceConnector` | `sql_select` input |
| `io.confluent.connect.jdbc.JdbcSinkConnector` | `sql_insert` output |
| `org.apache.kafka.connect.mirror.MirrorSourceConnector` | `kafka_franz` input + output |

**Converters** (`value.converter`)

| Converter | Result |
|---|---|
| `io.confluent.connect.avro.AvroConverter` | `schema_registry_decode` processor |
| `io.confluent.connect.protobuf.ProtobufConverter` | `schema_registry_decode` processor |
| `org.apache.kafka.connect.json.JsonConverter` | none (already structured) |
| `org.apache.kafka.connect.storage.StringConverter` | none |

**SMTs** (`transforms.*`), emitted as Bloblang `mapping` processors in declared order

| SMT | Mapping |
|---|---|
| `InsertField` | `root.<field> = "<value>"` |
| `ReplaceField` | renames → `root.<new> = this.<old>` + `root.<old> = deleted()`; excludes → `deleted()` |
| `RegexRouter` | rewrites the `kafka_topic` metadata via `re_replace_all` |
| `ExtractField` | `root = this.<field>` |
| `HoistField` | `root = {"<field>": this}` |
| `MaskField` | `root.<f> = ""` (or the `replacement`) for each masked field |
| `Cast` | per-field `root.<f> = this.<f>.number()` / `.string()` / `.bool()` |
| `TimestampConverter` | `root.<field> = this.<field>.ts_unix()` / `.ts_format(...)` / `.ts_parse(...)` (layout flagged with a TODO) |
| `ValueToKey` | `meta key = this.<field>.string()` |

Both the `$Value` and `$Key` class variants are registered. The generated Bloblang
targets the value document; for `$Key` variants the mapper adds a `# TODO` + warning
noting the transform should target the message key (RPCN sets keys via the output
`key` field / `meta key`, not the value).

Anything outside these lists (other connectors, converters, SMTs, or individual
fields) is emitted as a commented stub / TODO with a recorded warning.

## Output shape

```yaml
# Converted from Kafka Connect connector "j" (class=io.confluent.connect.jdbc.JdbcSourceConnector). Review # TODO markers.
input:
  sql_select:
    driver: postgres # TODO: set the database driver (e.g. postgres, mysql, mssql)
    dsn: "" # TODO: set the database DSN
    table: orders
    columns:
      - '*' # TODO: list specific columns if needed
output:
  stdout: {} # TODO: set the output destination
```

- A provenance header records the source connector and class.
- Sinks populate `output:` and leave an `input:` TODO stub; sources do the reverse;
  MirrorMaker populates both.
- Converter processors come first in `pipeline.processors`, then SMTs in order, so
  transforms run on decoded data (matching Kafka Connect runtime semantics).

## Not yet supported

- `.properties` files and pulling configs directly from a live Connect REST API
  (fetch the JSON yourself with `curl` for now).
- Connectors beyond the v1 list (e.g. Debezium CDC sources, Elasticsearch, HTTP).
- A machine-readable report file — v1 uses inline comments + an stderr summary.

## Architecture

Five stages, all in this package:

```
input bytes
  → parse()     normalize JSON → ConnectConfig{Name, Class, Props}   (parse.go)
  → registry    connector.class / converters / SMTs → mappers        (registry.go)
  → mappers     build yaml.Node fragments + record warnings          (conn_*.go, conv_*.go, smt_*.go)
  → assemble()  stitch into input / pipeline.processors / output      (assemble.go)
  → render()    encode to YAML with inline # TODO comments            (render.go)
```

- **`MapCtx`** (`mapctx.go`) is the per-conversion scratchpad: typed property
  getters that mark fields *consumed*, a `Warn(field, msg)` recorder, and an
  `Unmapped()` sweep that turns leftover properties into TODO warnings.
- The output is built as a `gopkg.in/yaml.v3` node tree so `# TODO` comments attach
  to the exact line they concern.
- Mappers register themselves via `init()`, so adding one never touches the engine.

## Extending

To add a connector:

1. Create `conn_<name>.go` with a type implementing
   `ConnectorMapper`:
   ```go
   func (myConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error)
   ```
   Read properties via `ctx.String(...)` (this marks them consumed), build the
   component body with the `mapping`/`scalar`/`kv`/`component` helpers
   (`yamlutil.go`), emit `# TODO` LineComments and `ctx.Warn(...)` for required
   fields you can't infer, and return `Component{Output: ...}` (or `Input`, or
   both).
2. Register it in `init()`:
   ```go
   func init() { registerConnector("the.kafka.connect.Class", myConnector{}) }
   ```
3. Add `conn_<name>_test.go` with a test that calls `Convert(...)` and then
   `assertValidRPCN(t, res.YAML)` — see below.

Converters (`ConverterMapper`) and SMTs (`SMTMapper`) follow the same pattern with
`registerConverter` / `registerSMT`.

## Testing

Run the suite:

```bash
go test ./internal/connect_converter/
```

- **Linter-backed validation.** The shared helper `assertValidRPCN` (in
  `linter_test.go`) feeds every generated config through the real benthos
  `service.NewStreamBuilder().SetYAML(...)`, proving the output is valid Redpanda
  Connect config against the actual component schemas — not just well-formed YAML.
  `linter_test.go` blank-imports the component bundles, so per-mapper tests don't
  manage imports.
- **Golden files.** `golden_test.go` pins exact output for 9 end-to-end cases under
  `testdata/`. Regenerate after an intentional change with:
  ```bash
  go test ./internal/connect_converter/ -run TestGolden -update
  ```
  Always read the regenerated YAML before committing it.
