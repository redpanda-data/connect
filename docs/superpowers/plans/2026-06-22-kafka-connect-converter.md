# Kafka Connect → Redpanda Connect Converter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic Go engine (`internal/connect_converter`) plus a thin `convert` CLI subcommand that turns a Kafka Connect connector config into an equivalent Redpanda Connect (RPCN) pipeline YAML, with inline `# TODO` markers for anything it cannot fully map.

**Architecture:** A pure library package takes config bytes in and returns `Result{YAML, Warnings}`. Input JSON (REST-wrapped or flat) is normalized to a `ConnectConfig`. A registry dispatches `connector.class` to a `ConnectorMapper`, `key/value.converter` to `ConverterMapper`s, and each `transforms.*` SMT to an `SMTMapper`. Mappers build a `gopkg.in/yaml.v3` node tree so `# TODO` comments attach to exact lines. Unknown content degrades to commented stubs — never a hard failure. The CLI layer owns all I/O.

**Tech Stack:** Go, `gopkg.in/yaml.v3` v3.0.1 (node tree + comments), `github.com/urfave/cli/v2` (CLI), `github.com/redpanda-data/benthos/v4` `public/service` (config validation in tests), `testify` (assertions).

## Global Constraints

- Module path: `github.com/redpanda-data/connect/v4`.
- Engine directory: `internal/connect_converter/`. **Package name: `connectconverter`** (no underscore — Go-idiomatic; the directory keeps the underscore the user chose). Import as `connectconverter "github.com/redpanda-data/connect/v4/internal/connect_converter"`.
- YAML library: `gopkg.in/yaml.v3` (already a dependency at v3.0.1). Do not add new YAML deps.
- Benthos service API for validation: `github.com/redpanda-data/benthos/v4/public/service` (v4.75.0, already a dependency).
- Mappers live as **files within the single `connectconverter` package** (e.g. `conn_s3.go`, `smt_insertfield.go`), NOT in sub-packages — sub-packages would create an import cycle with the registry. Register via `init()`.
- Output YAML uses 2-space indentation (`yaml.Encoder.SetIndent(2)`).
- Conversion never hard-fails on unmapped content: emit valid YAML + `# TODO` + a `Warning`. Hard `error` only for malformed JSON or missing `connector.class`.
- Run `task fmt` (gofumpt) before every commit. Run `task lint` before the final commit.
- License headers: copy the exact header block from a sibling file in the same distribution (see Task 1, Step 1).

---

## File Structure

```
internal/connect_converter/
  types.go        # ConnectConfig, Warning, Result, Component, ConverterRole, SMTConfig
  parse.go        # parse(): REST-wrapped or flat JSON → ConnectConfig
  mapctx.go       # MapCtx: warnings, consumed-field tracking, typed getters
  yamlutil.go     # scalar/mapping/seq/kv/component node builders
  registry.go     # mapper interfaces, registries, register/lookup, fallback connector
  assemble.go     # assemble(): build the RPCN stream node tree + attach unmapped TODOs
  render.go       # render(): encode node tree to YAML bytes
  convert.go      # Convert(): top-level orchestration; mapConverters/mapSMTs helpers
  conn_s3.go      # S3 sink connector mapper
  conn_gcs.go     # GCS sink connector mapper
  conn_bigquery.go# BigQuery sink connector mapper
  conn_snowflake.go # Snowflake sink connector mapper
  conn_jdbc.go    # JDBC source + sink connector mappers
  conn_mirror.go  # MirrorMaker source connector mapper
  conv_serializers.go # avro/json/string/protobuf converter mappers
  smt_insertfield.go  # InsertField SMT mapper
  smt_replacefield.go # ReplaceField SMT mapper
  smt_regexrouter.go  # RegexRouter SMT mapper
  *_test.go       # per-file unit tests + golden tests
  testdata/       # <case>.input.json + <case>.expected.yaml golden fixtures

internal/cli/convert.go   # thin `convert` subcommand
internal/cli/enterprise.go (modify ~line 242: register convertCli())
```

---

### Task 1: Core types and input parser

**Files:**
- Create: `internal/connect_converter/types.go`
- Create: `internal/connect_converter/parse.go`
- Test: `internal/connect_converter/parse_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `type ConnectConfig struct { Name string; Class string; Props map[string]any }`
  - `type Warning struct { Field string; Message string }`
  - `type Result struct { YAML []byte; Warnings []Warning }`
  - `type Component struct { Input *yaml.Node; Output *yaml.Node }`
  - `type ConverterRole int` with `KeyConverter ConverterRole = iota; ValueConverter`
  - `type SMTConfig struct { Alias string; Type string; Props map[string]any }`
  - `func parse(input []byte) (ConnectConfig, error)`

- [ ] **Step 1: Create `types.go` with the license header and type definitions**

First copy the license header: open any existing file in `internal/impl/kafka/` (e.g. `internal/impl/kafka/franz_client.go`), copy its top comment block verbatim into the new files. Then:

```go
// <PASTE EXACT LICENSE HEADER FROM A SIBLING FILE HERE>

package connectconverter

import "gopkg.in/yaml.v3"

// ConnectConfig is a normalized Kafka Connect connector configuration.
type ConnectConfig struct {
	Name  string
	Class string
	Props map[string]any
}

// Warning records something the converter could not fully map.
type Warning struct {
	Field   string
	Message string
}

// Result is the output of a conversion.
type Result struct {
	YAML     []byte
	Warnings []Warning
}

// Component is the input and/or output a connector maps to. A source sets
// Input, a sink sets Output, MirrorMaker sets both. A nil side becomes a
// commented TODO stub during assembly.
type Component struct {
	Input  *yaml.Node
	Output *yaml.Node
}

// ConverterRole distinguishes key vs value converters.
type ConverterRole int

const (
	KeyConverter ConverterRole = iota
	ValueConverter
)

// Prefix returns the Kafka Connect property prefix for this role.
func (r ConverterRole) Prefix() string {
	if r == KeyConverter {
		return "key.converter"
	}
	return "value.converter"
}

// SMTConfig is a single resolved transform (transforms.<alias>.*).
type SMTConfig struct {
	Alias string
	Type  string
	Props map[string]any // keys with the "transforms.<alias>." prefix stripped
}
```

- [ ] **Step 2: Write failing tests in `parse_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRESTWrapped(t *testing.T) {
	in := []byte(`{"name":"my-conn","config":{"connector.class":"io.example.Foo","topics":"orders"}}`)
	cfg, err := parse(in)
	require.NoError(t, err)
	assert.Equal(t, "my-conn", cfg.Name)
	assert.Equal(t, "io.example.Foo", cfg.Class)
	assert.Equal(t, "orders", cfg.Props["topics"])
	// connector.class must NOT remain a stray prop key beyond Class.
	assert.Equal(t, "io.example.Foo", cfg.Props["connector.class"])
}

func TestParseFlat(t *testing.T) {
	in := []byte(`{"connector.class":"io.example.Foo","name":"my-conn","topics":"orders"}`)
	cfg, err := parse(in)
	require.NoError(t, err)
	assert.Equal(t, "my-conn", cfg.Name)
	assert.Equal(t, "io.example.Foo", cfg.Class)
	assert.Equal(t, "orders", cfg.Props["topics"])
}

func TestParseMalformed(t *testing.T) {
	_, err := parse([]byte(`{not json`))
	require.Error(t, err)
}

func TestParseMissingClass(t *testing.T) {
	_, err := parse([]byte(`{"name":"x","config":{"topics":"orders"}}`))
	require.Error(t, err)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/connect_converter/ -run TestParse -v`
Expected: FAIL — `undefined: parse`.

- [ ] **Step 4: Implement `parse.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"encoding/json"
	"errors"
	"fmt"
)

// parse normalizes a Kafka Connect config. It accepts the REST-wrapped form
// ({"name":..., "config":{...}}) and the flat form (a bare property map).
func parse(input []byte) (ConnectConfig, error) {
	var raw map[string]any
	if err := json.Unmarshal(input, &raw); err != nil {
		return ConnectConfig{}, fmt.Errorf("invalid JSON: %w", err)
	}

	props := raw
	name, _ := raw["name"].(string)

	// REST-wrapped form: unwrap "config".
	if cfg, ok := raw["config"].(map[string]any); ok {
		props = cfg
	}

	class, _ := props["connector.class"].(string)
	if class == "" {
		return ConnectConfig{}, errors.New("missing required field: connector.class")
	}

	// Prefer the wrapper name; fall back to a name inside config.
	if name == "" {
		name, _ = props["name"].(string)
	}

	return ConnectConfig{Name: name, Class: class, Props: props}, nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/connect_converter/ -run TestParse -v`
Expected: PASS (all four).

- [ ] **Step 6: Format and commit**

```bash
task fmt
git add internal/connect_converter/types.go internal/connect_converter/parse.go internal/connect_converter/parse_test.go
git commit -m "feat(connect_converter): core types and input parser"
```

---

### Task 2: MapCtx — warnings, consumed-field tracking, typed getters

**Files:**
- Create: `internal/connect_converter/mapctx.go`
- Test: `internal/connect_converter/mapctx_test.go`

**Interfaces:**
- Consumes: `ConnectConfig`, `Warning` (Task 1).
- Produces:
  - `func newMapCtx(cfg ConnectConfig) *MapCtx`
  - `func (c *MapCtx) Warn(field, msg string)`
  - `func (c *MapCtx) String(key string) (string, bool)` — returns the prop as a string, marks it consumed.
  - `func (c *MapCtx) consume(key string)` — marks a key consumed without reading.
  - `func (c *MapCtx) Warnings() []Warning`
  - `func (c *MapCtx) Unmapped() []string` — sorted prop keys never consumed, excluding meta keys.

- [ ] **Step 1: Write failing tests in `mapctx_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestCtx(props map[string]any) *MapCtx {
	return newMapCtx(ConnectConfig{Name: "c", Class: "io.example.Foo", Props: props})
}

func TestMapCtxStringConsumes(t *testing.T) {
	c := newTestCtx(map[string]any{"a": "x", "b": "y"})
	v, ok := c.String("a")
	assert.True(t, ok)
	assert.Equal(t, "x", v)
	// "a" consumed, "b" not.
	assert.Equal(t, []string{"b"}, c.Unmapped())
}

func TestMapCtxStringMissing(t *testing.T) {
	c := newTestCtx(map[string]any{})
	_, ok := c.String("missing")
	assert.False(t, ok)
}

func TestMapCtxUnmappedExcludesMeta(t *testing.T) {
	c := newTestCtx(map[string]any{
		"connector.class": "io.example.Foo",
		"name":            "c",
		"tasks.max":       "1",
		"transforms":      "a",
		"key.converter":   "x",
		"value.converter": "y",
		"real.field":      "keep",
	})
	assert.Equal(t, []string{"real.field"}, c.Unmapped())
}

func TestMapCtxWarn(t *testing.T) {
	c := newTestCtx(map[string]any{})
	c.Warn("f", "bad")
	assert.Equal(t, []Warning{{Field: "f", Message: "bad"}}, c.Warnings())
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/connect_converter/ -run TestMapCtx -v`
Expected: FAIL — `undefined: newMapCtx`.

- [ ] **Step 3: Implement `mapctx.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"fmt"
	"sort"
	"strings"
)

// metaKeys are Kafka Connect properties handled outside connector mappers
// (or framework-level) and therefore never reported as "unmapped".
var metaKeys = map[string]bool{
	"connector.class": true,
	"name":            true,
	"tasks.max":       true,
	"transforms":      true,
}

// metaPrefixes are property prefixes handled by converters/SMTs.
var metaPrefixes = []string{"key.converter", "value.converter", "header.converter", "transforms."}

// MapCtx is the per-conversion scratchpad threaded through every mapper.
type MapCtx struct {
	cfg      ConnectConfig
	warnings []Warning
	consumed map[string]bool
}

func newMapCtx(cfg ConnectConfig) *MapCtx {
	return &MapCtx{cfg: cfg, consumed: map[string]bool{}}
}

// Warn records a warning. Callers that want an inline TODO should also attach
// a comment to the relevant yaml.Node.
func (c *MapCtx) Warn(field, msg string) {
	c.warnings = append(c.warnings, Warning{Field: field, Message: msg})
}

// Warnings returns all recorded warnings.
func (c *MapCtx) Warnings() []Warning { return c.warnings }

// String returns the prop value as a string and marks the key consumed.
func (c *MapCtx) String(key string) (string, bool) {
	c.consumed[key] = true
	v, ok := c.cfg.Props[key]
	if !ok {
		return "", false
	}
	return fmt.Sprint(v), true
}

// consume marks a key consumed without reading it.
func (c *MapCtx) consume(key string) { c.consumed[key] = true }

// isMeta reports whether a key is framework/meta and should be ignored by the
// unmapped sweep.
func isMeta(key string) bool {
	if metaKeys[key] {
		return true
	}
	for _, p := range metaPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// Unmapped returns sorted prop keys that were never consumed and are not meta.
func (c *MapCtx) Unmapped() []string {
	var out []string
	for k := range c.cfg.Props {
		if c.consumed[k] || isMeta(k) {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/connect_converter/ -run TestMapCtx -v`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/mapctx.go internal/connect_converter/mapctx_test.go
git commit -m "feat(connect_converter): MapCtx with warnings and unmapped-field tracking"
```

---

### Task 3: YAML node builders

**Files:**
- Create: `internal/connect_converter/yamlutil.go`
- Test: `internal/connect_converter/yamlutil_test.go`

**Interfaces:**
- Consumes: `gopkg.in/yaml.v3`.
- Produces:
  - `func scalar(val string) *yaml.Node`
  - `func mapping(pairs ...*yaml.Node) *yaml.Node`
  - `func seq(items ...*yaml.Node) *yaml.Node`
  - `func kv(m *yaml.Node, key string, val *yaml.Node)` — appends a key/value pair to a mapping node.
  - `func component(name string, body *yaml.Node) *yaml.Node` — returns `mapping(scalar(name), body)`.

- [ ] **Step 1: Write failing tests in `yamlutil_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestComponentRendersSingleKeyMap(t *testing.T) {
	body := mapping()
	kv(body, "bucket", scalar("my-bucket"))
	comp := component("aws_s3", body)

	out, err := yaml.Marshal(comp)
	require.NoError(t, err)
	assert.Equal(t, "aws_s3:\n    bucket: my-bucket\n", string(out))
}

func TestSeqBuildsSequence(t *testing.T) {
	s := seq(scalar("a"), scalar("b"))
	out, err := yaml.Marshal(s)
	require.NoError(t, err)
	assert.Equal(t, "- a\n- b\n", string(out))
}
```

(Note: default `yaml.Marshal` indents 4 spaces; the engine encoder in Task 5 sets 2. These tests assert structure, not engine indentation.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/connect_converter/ -run 'TestComponent|TestSeq' -v`
Expected: FAIL — `undefined: mapping`.

- [ ] **Step 3: Implement `yamlutil.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import "gopkg.in/yaml.v3"

func scalar(val string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: val}
}

func mapping(pairs ...*yaml.Node) *yaml.Node {
	return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: pairs}
}

func seq(items ...*yaml.Node) *yaml.Node {
	return &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: items}
}

// kv appends a key/value pair to a mapping node.
func kv(m *yaml.Node, key string, val *yaml.Node) {
	m.Content = append(m.Content, scalar(key), val)
}

// component wraps a body map as a single-key map {name: body}, the shape RPCN
// uses for an input/output/processor.
func component(name string, body *yaml.Node) *yaml.Node {
	return mapping(scalar(name), body)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/connect_converter/ -run 'TestComponent|TestSeq' -v`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/yamlutil.go internal/connect_converter/yamlutil_test.go
git commit -m "feat(connect_converter): yaml node builder helpers"
```

---

### Task 4: Registry, mapper interfaces, and fallback connector

**Files:**
- Create: `internal/connect_converter/registry.go`
- Test: `internal/connect_converter/registry_test.go`

**Interfaces:**
- Consumes: `ConnectConfig`, `Component`, `ConverterRole`, `SMTConfig`, `MapCtx` (Tasks 1–2); `yamlutil` (Task 3).
- Produces:
  - `type ConnectorMapper interface { Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) }`
  - `type ConverterMapper interface { Map(role ConverterRole, ctx *MapCtx) ([]*yaml.Node, error) }`
  - `type SMTMapper interface { Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) }`
  - `func registerConnector(class string, m ConnectorMapper)`
  - `func registerConverter(class string, m ConverterMapper)`
  - `func registerSMT(typ string, m SMTMapper)`
  - `func lookupConnector(class string) ConnectorMapper` — returns `fallbackConnector{}` on miss.
  - `func lookupConverter(class string) (ConverterMapper, bool)`
  - `func lookupSMT(typ string) (SMTMapper, bool)`

- [ ] **Step 1: Write failing tests in `registry_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLookupConnectorMiss_ReturnsFallback(t *testing.T) {
	m := lookupConnector("does.not.Exist")
	ctx := newTestCtx(map[string]any{"connector.class": "does.not.Exist"})
	comp, err := m.Map(ctx.cfg, ctx)
	require.NoError(t, err)
	require.NotNil(t, comp.Output)

	out, err := yaml.Marshal(comp.Output)
	require.NoError(t, err)
	assert.Contains(t, string(out), "TODO")
	assert.NotEmpty(t, ctx.Warnings())
}

func TestRegisterAndLookupConnector(t *testing.T) {
	registerConnector("test.Only", stubConnector{})
	m := lookupConnector("test.Only")
	_, ok := m.(stubConnector)
	assert.True(t, ok)
}

type stubConnector struct{}

func (stubConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	return Component{Output: component("drop", mapping())}, nil
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/connect_converter/ -run 'TestLookupConnector|TestRegister' -v`
Expected: FAIL — `undefined: lookupConnector`.

- [ ] **Step 3: Implement `registry.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// ConnectorMapper maps a connector.class to an RPCN input and/or output.
type ConnectorMapper interface {
	Map(cfg ConnectConfig, ctx *MapCtx) (Component, error)
}

// ConverterMapper maps a key/value.converter to deserialization processors.
type ConverterMapper interface {
	Map(role ConverterRole, ctx *MapCtx) ([]*yaml.Node, error)
}

// SMTMapper maps a single SMT to ordered processor nodes.
type SMTMapper interface {
	Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error)
}

var (
	connectorMappers = map[string]ConnectorMapper{}
	converterMappers = map[string]ConverterMapper{}
	smtMappers       = map[string]SMTMapper{}
)

func registerConnector(class string, m ConnectorMapper) { connectorMappers[class] = m }
func registerConverter(class string, m ConverterMapper) { converterMappers[class] = m }
func registerSMT(typ string, m SMTMapper)               { smtMappers[typ] = m }

func lookupConnector(class string) ConnectorMapper {
	if m, ok := connectorMappers[class]; ok {
		return m
	}
	return fallbackConnector{}
}

func lookupConverter(class string) (ConverterMapper, bool) {
	m, ok := converterMappers[class]
	return m, ok
}

func lookupSMT(typ string) (SMTMapper, bool) {
	m, ok := smtMappers[typ]
	return m, ok
}

// fallbackConnector emits a commented stub for unknown connector classes so
// conversion never hard-fails.
type fallbackConnector struct{}

func (fallbackConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	ctx.Warn("connector.class", fmt.Sprintf("unsupported connector class %q", cfg.Class))
	body := mapping()
	body.LineComment = fmt.Sprintf("TODO: unsupported connector.class=%s — map manually", cfg.Class)
	return Component{Output: component("drop", body)}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/connect_converter/ -run 'TestLookupConnector|TestRegister' -v`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/registry.go internal/connect_converter/registry_test.go
git commit -m "feat(connect_converter): mapper registry and fallback connector"
```

---

### Task 5: Assembly, rendering, and end-to-end Convert (with fallback only)

This task wires the skeleton: `Convert` of an unknown connector produces valid YAML with a provenance header, input/output, and TODO stubs. Converters/SMTs are stubbed as no-ops here and filled in Tasks 11–12.

**Files:**
- Create: `internal/connect_converter/assemble.go`
- Create: `internal/connect_converter/render.go`
- Create: `internal/connect_converter/convert.go`
- Test: `internal/connect_converter/convert_test.go`

**Interfaces:**
- Consumes: everything from Tasks 1–4.
- Produces:
  - `func assemble(cfg ConnectConfig, comp Component, procs []*yaml.Node, unmapped []string) *yaml.Node`
  - `func render(root *yaml.Node) ([]byte, error)`
  - `func Convert(input []byte) (*Result, error)` (exported entry point)
  - `func mapConverters(ctx *MapCtx) []*yaml.Node` (returns nil until Task 11)
  - `func mapSMTs(ctx *MapCtx) []*yaml.Node` (returns nil until Task 12)

- [ ] **Step 1: Write failing test in `convert_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertValidRPCN parses YAML through the benthos stream builder to prove it is
// valid Redpanda Connect config, not just well-formed YAML.
func assertValidRPCN(t *testing.T, yamlBytes []byte) {
	t.Helper()
	b := service.NewStreamBuilder()
	require.NoError(t, b.SetYAML(string(yamlBytes)), "generated YAML is not valid RPCN config:\n%s", yamlBytes)
}

func TestConvertUnknownConnector(t *testing.T) {
	in := []byte(`{"name":"mystery","config":{"connector.class":"com.acme.Mystery","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)

	y := string(res.YAML)
	assert.Contains(t, y, "Converted from Kafka Connect connector \"mystery\"")
	assert.Contains(t, y, "TODO: unsupported connector.class=com.acme.Mystery")
	// "topics" is an unmapped field for the fallback connector.
	assert.Contains(t, y, "topics")
	assert.NotEmpty(t, res.Warnings)
}

func TestConvertMalformedReturnsError(t *testing.T) {
	_, err := Convert([]byte(`{bad`))
	require.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/connect_converter/ -run TestConvert -v`
Expected: FAIL — `undefined: Convert`.

- [ ] **Step 3: Implement `assemble.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// assemble builds the RPCN stream node tree. A nil Input/Output side becomes a
// commented TODO stub. Unmapped field TODOs are attached to the primary
// component node (output if present, else input).
func assemble(cfg ConnectConfig, comp Component, procs []*yaml.Node, unmapped []string) *yaml.Node {
	root := mapping()
	root.HeadComment = fmt.Sprintf(
		"Converted from Kafka Connect connector %q (class=%s). Review # TODO markers.",
		cfg.Name, cfg.Class,
	)

	input := comp.Input
	if input == nil {
		stub := mapping()
		stub.LineComment = "TODO: set the input that feeds this pipeline (e.g. your source topic)"
		input = component("stdin", stub)
	}
	kv(root, "input", input)

	if len(procs) > 0 {
		pipeline := mapping()
		kv(pipeline, "processors", seq(procs...))
		kv(root, "pipeline", pipeline)
	}

	output := comp.Output
	if output == nil {
		stub := mapping()
		stub.LineComment = "TODO: set the output destination"
		output = component("stdout", stub)
	}
	kv(root, "output", output)

	// Attach unmapped-field TODOs to the primary component's body node.
	primary := output
	if comp.Output == nil && comp.Input != nil {
		primary = input
	}
	if len(unmapped) > 0 && len(primary.Content) == 2 {
		body := primary.Content[1] // the component body map
		var lines []string
		for _, k := range unmapped {
			lines = append(lines, fmt.Sprintf("TODO: unmapped field %s=%v", k, cfg.Props[k]))
		}
		if body.HeadComment != "" {
			body.HeadComment += "\n"
		}
		body.HeadComment += strings.Join(lines, "\n")
	}

	return root
}

var _ = yaml.MappingNode // keep yaml import if unused after edits
```

(Remove the trailing `var _` line once other code references `yaml`.)

- [ ] **Step 4: Implement `render.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"bytes"

	"gopkg.in/yaml.v3"
)

// render encodes the node tree to YAML with 2-space indentation.
func render(root *yaml.Node) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	doc := &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}}
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
```

- [ ] **Step 5: Implement `convert.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import "gopkg.in/yaml.v3"

// Convert turns a Kafka Connect connector config (REST-wrapped or flat JSON)
// into an equivalent Redpanda Connect pipeline YAML.
func Convert(input []byte) (*Result, error) {
	cfg, err := parse(input)
	if err != nil {
		return nil, err
	}

	ctx := newMapCtx(cfg)

	comp, err := lookupConnector(cfg.Class).Map(cfg, ctx)
	if err != nil {
		return nil, err
	}

	var procs []*yaml.Node
	procs = append(procs, mapConverters(ctx)...)
	procs = append(procs, mapSMTs(ctx)...)

	unmapped := ctx.Unmapped()
	for _, k := range unmapped {
		ctx.Warn(k, "unmapped field")
	}

	root := assemble(cfg, comp, procs, unmapped)
	out, err := render(root)
	if err != nil {
		return nil, err
	}
	return &Result{YAML: out, Warnings: ctx.Warnings()}, nil
}

// mapConverters maps key/value.converter to processors. Filled in Task 11.
func mapConverters(ctx *MapCtx) []*yaml.Node { return nil }

// mapSMTs maps transforms.* to processors in order. Filled in Task 12.
func mapSMTs(ctx *MapCtx) []*yaml.Node { return nil }
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/connect_converter/ -run TestConvert -v`
Expected: PASS. (`TestConvertUnknownConnector` proves header, TODO stub, and unmapped `topics` appear; `TestConvertMalformedReturnsError` proves hard error.)

- [ ] **Step 7: Format and commit**

```bash
task fmt
git add internal/connect_converter/assemble.go internal/connect_converter/render.go internal/connect_converter/convert.go internal/connect_converter/convert_test.go
git commit -m "feat(connect_converter): assembly, rendering, end-to-end Convert skeleton"
```

---

### Task 6: S3 sink connector mapper

**Files:**
- Create: `internal/connect_converter/conn_s3.go`
- Test: `internal/connect_converter/conn_s3_test.go`
- Create: `internal/connect_converter/testdata/s3_sink.input.json`

**Interfaces:**
- Consumes: `ConnectorMapper`, `registerConnector`, `Component`, `MapCtx`, yaml builders.
- Produces: registration of `io.confluent.connect.s3.S3SinkConnector`.

KC → RPCN field mapping (`aws_s3` output): `s3.bucket.name`→`bucket`; `s3.region`/`aws.s3.region`→`region`; partitioning/format → a generated `path`. `topics` is consumed (used in the path interpolation) so it is not reported unmapped.

- [ ] **Step 1: Write failing test in `conn_s3_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertS3Sink(t *testing.T) {
	in := []byte(`{
	  "name":"s3-sink",
	  "config":{
	    "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	    "s3.bucket.name":"my-bucket",
	    "s3.region":"us-east-1",
	    "topics":"orders"
	  }
	}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)

	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "aws_s3:")
	assert.Contains(t, y, "bucket: my-bucket")
	assert.Contains(t, y, "region: us-east-1")
	assert.Contains(t, y, "path:")
	// input side is a TODO stub for a sink.
	assert.Contains(t, y, "TODO: set the input")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/connect_converter/ -run TestConvertS3Sink -v`
Expected: FAIL — output is the fallback stub, not `aws_s3`.

- [ ] **Step 3: Implement `conn_s3.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

func init() {
	registerConnector("io.confluent.connect.s3.S3SinkConnector", s3SinkConnector{})
}

type s3SinkConnector struct{}

func (s3SinkConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	if v, ok := ctx.String("s3.bucket.name"); ok {
		kv(body, "bucket", scalar(v))
	} else {
		ctx.Warn("s3.bucket.name", "missing required bucket name")
		stub := scalar("")
		stub.LineComment = "TODO: set the S3 bucket name"
		kv(body, "bucket", stub)
	}

	if v, ok := ctx.String("s3.region"); ok {
		kv(body, "region", scalar(v))
	} else if v, ok := ctx.String("aws.s3.region"); ok {
		kv(body, "region", scalar(v))
	}

	// Build an object path from the source topic. KC routes by topic; RPCN
	// uses interpolation on the kafka_topic metadata.
	ctx.consume("topics")
	path := scalar(`${! @kafka_topic }/${! timestamp_unix() }-${! uuid_v4() }.json`)
	path.LineComment = "TODO: review object path/partitioning"
	kv(body, "path", path)

	return Component{Output: component("aws_s3", body)}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/connect_converter/ -run TestConvertS3Sink -v`
Expected: PASS.

- [ ] **Step 5: Add golden fixture and commit**

Create `testdata/s3_sink.input.json` with the config from Step 1 (the `config` map contents, flat form is fine). Then:

```bash
task fmt
git add internal/connect_converter/conn_s3.go internal/connect_converter/conn_s3_test.go internal/connect_converter/testdata/s3_sink.input.json
git commit -m "feat(connect_converter): S3 sink connector mapper"
```

---

### Task 7: GCS sink connector mapper

**Files:**
- Create: `internal/connect_converter/conn_gcs.go`
- Test: `internal/connect_converter/conn_gcs_test.go`

**Interfaces:**
- Produces: registration of `io.confluent.connect.gcs.GcsSinkConnector` → `gcp_cloud_storage` output.

KC → RPCN (`gcp_cloud_storage`): `gcs.bucket.name`→`bucket`; topic → `path`.

- [ ] **Step 1: Write failing test in `conn_gcs_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertGCSSink(t *testing.T) {
	in := []byte(`{"name":"gcs","config":{"connector.class":"io.confluent.connect.gcs.GcsSinkConnector","gcs.bucket.name":"bkt","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "gcp_cloud_storage:")
	assert.Contains(t, y, "bucket: bkt")
	assert.Contains(t, y, "path:")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/connect_converter/ -run TestConvertGCSSink -v`
Expected: FAIL — fallback stub.

- [ ] **Step 3: Implement `conn_gcs.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

func init() {
	registerConnector("io.confluent.connect.gcs.GcsSinkConnector", gcsSinkConnector{})
}

type gcsSinkConnector struct{}

func (gcsSinkConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	if v, ok := ctx.String("gcs.bucket.name"); ok {
		kv(body, "bucket", scalar(v))
	} else {
		ctx.Warn("gcs.bucket.name", "missing required bucket name")
		stub := scalar("")
		stub.LineComment = "TODO: set the GCS bucket name"
		kv(body, "bucket", stub)
	}

	ctx.consume("topics")
	path := scalar(`${! @kafka_topic }/${! timestamp_unix() }-${! uuid_v4() }.json`)
	path.LineComment = "TODO: review object path/partitioning"
	kv(body, "path", path)

	return Component{Output: component("gcp_cloud_storage", body)}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/connect_converter/ -run TestConvertGCSSink -v`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/conn_gcs.go internal/connect_converter/conn_gcs_test.go
git commit -m "feat(connect_converter): GCS sink connector mapper"
```

---

### Task 8: BigQuery sink connector mapper

**Files:**
- Create: `internal/connect_converter/conn_bigquery.go`
- Test: `internal/connect_converter/conn_bigquery_test.go`

**Interfaces:**
- Produces: registration of `com.wepay.kafka.connect.bigquery.BigQuerySinkConnector` → `gcp_bigquery` output.

KC → RPCN (`gcp_bigquery`): `project`→`project`; `defaultDataset`→`dataset`; `topics`→`table` (best-effort, KC derives table from topic).

- [ ] **Step 1: Write failing test in `conn_bigquery_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertBigQuerySink(t *testing.T) {
	in := []byte(`{"name":"bq","config":{"connector.class":"com.wepay.kafka.connect.bigquery.BigQuerySinkConnector","project":"proj","defaultDataset":"ds","topics":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "gcp_bigquery:")
	assert.Contains(t, y, "project: proj")
	assert.Contains(t, y, "dataset: ds")
	assert.Contains(t, y, "table:")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/connect_converter/ -run TestConvertBigQuerySink -v`
Expected: FAIL — fallback stub.

- [ ] **Step 3: Implement `conn_bigquery.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

func init() {
	registerConnector("com.wepay.kafka.connect.bigquery.BigQuerySinkConnector", bigQuerySinkConnector{})
}

type bigQuerySinkConnector struct{}

func (bigQuerySinkConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	if v, ok := ctx.String("project"); ok {
		kv(body, "project", scalar(v))
	} else {
		ctx.Warn("project", "missing required project")
		stub := scalar("")
		stub.LineComment = "TODO: set the GCP project"
		kv(body, "project", stub)
	}

	if v, ok := ctx.String("defaultDataset"); ok {
		kv(body, "dataset", scalar(v))
	} else if v, ok := ctx.String("datasets"); ok {
		kv(body, "dataset", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the BigQuery dataset"
		kv(body, "dataset", stub)
	}

	ctx.consume("topics")
	table := scalar("")
	table.LineComment = "TODO: set the destination table (KC derives it from the topic)"
	kv(body, "table", table)

	return Component{Output: component("gcp_bigquery", body)}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/connect_converter/ -run TestConvertBigQuerySink -v`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/conn_bigquery.go internal/connect_converter/conn_bigquery_test.go
git commit -m "feat(connect_converter): BigQuery sink connector mapper"
```

---

### Task 9: Snowflake sink connector mapper

**Files:**
- Create: `internal/connect_converter/conn_snowflake.go`
- Test: `internal/connect_converter/conn_snowflake_test.go`

**Interfaces:**
- Produces: registration of `com.snowflake.kafka.connector.SnowflakeSinkConnector` → `snowflake_streaming` output.

KC → RPCN (`snowflake_streaming`): `snowflake.url.name`→`account` (TODO: hostname vs account id); `snowflake.user.name`→`user`; `snowflake.database.name`→`database`; `snowflake.schema.name`→`schema`; `snowflake.private.key`→`private_key`; `snowflake.role.name`→`role`.

- [ ] **Step 1: Write failing test in `conn_snowflake_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertSnowflakeSink(t *testing.T) {
	in := []byte(`{"name":"sf","config":{
	  "connector.class":"com.snowflake.kafka.connector.SnowflakeSinkConnector",
	  "snowflake.url.name":"acct.snowflakecomputing.com",
	  "snowflake.user.name":"svc",
	  "snowflake.database.name":"DB",
	  "snowflake.schema.name":"PUBLIC",
	  "snowflake.private.key":"KEY",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "snowflake_streaming:")
	assert.Contains(t, y, "user: svc")
	assert.Contains(t, y, "database: DB")
	assert.Contains(t, y, "schema: PUBLIC")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/connect_converter/ -run TestConvertSnowflakeSink -v`
Expected: FAIL — fallback stub.

- [ ] **Step 3: Implement `conn_snowflake.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

func init() {
	registerConnector("com.snowflake.kafka.connector.SnowflakeSinkConnector", snowflakeSinkConnector{})
}

type snowflakeSinkConnector struct{}

func (snowflakeSinkConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	// account: KC provides a full URL/host; RPCN wants the account identifier.
	if v, ok := ctx.String("snowflake.url.name"); ok {
		acc := scalar(v)
		acc.LineComment = "TODO: RPCN expects the account identifier, not the full URL"
		kv(body, "account", acc)
	}

	mapStr := func(kcKey, rpKey string) {
		if v, ok := ctx.String(kcKey); ok {
			kv(body, rpKey, scalar(v))
		}
	}
	mapStr("snowflake.user.name", "user")
	mapStr("snowflake.role.name", "role")
	mapStr("snowflake.database.name", "database")
	mapStr("snowflake.schema.name", "schema")
	mapStr("snowflake.private.key", "private_key")

	ctx.consume("topics")
	table := scalar("")
	table.LineComment = "TODO: set the destination table (KC derives it from the topic)"
	kv(body, "table", table)

	return Component{Output: component("snowflake_streaming", body)}, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/connect_converter/ -run TestConvertSnowflakeSink -v`
Expected: PASS. If the linter rejects a field name, run `rpk connect list outputs` or check `internal/impl/snowflake` for the exact `snowflake_streaming` field names and adjust `mapStr` targets, then re-run.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/conn_snowflake.go internal/connect_converter/conn_snowflake_test.go
git commit -m "feat(connect_converter): Snowflake sink connector mapper"
```

---

### Task 10: JDBC source and sink connector mappers

**Files:**
- Create: `internal/connect_converter/conn_jdbc.go`
- Test: `internal/connect_converter/conn_jdbc_test.go`

**Interfaces:**
- Produces: registration of `io.confluent.connect.jdbc.JdbcSourceConnector` → `sql_select` input, and `io.confluent.connect.jdbc.JdbcSinkConnector` → `sql_insert` output. Helper `func jdbcDriver(url string) string`.

KC → RPCN: `connection.url` → `driver` (derived) + `dsn` (URL minus `jdbc:` prefix); source `table.whitelist` → `table`; sink `table.name.format` → `table`.

- [ ] **Step 1: Write failing tests in `conn_jdbc_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJDBCDriver(t *testing.T) {
	assert.Equal(t, "postgres", jdbcDriver("jdbc:postgresql://h:5432/db"))
	assert.Equal(t, "mysql", jdbcDriver("jdbc:mysql://h:3306/db"))
	assert.Equal(t, "", jdbcDriver("weird"))
}

func TestConvertJDBCSource(t *testing.T) {
	in := []byte(`{"name":"jdbc-src","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSourceConnector","connection.url":"jdbc:postgresql://h:5432/db","table.whitelist":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "sql_select:")
	assert.Contains(t, y, "driver: postgres")
	assert.Contains(t, y, "table: orders")
}

func TestConvertJDBCSink(t *testing.T) {
	in := []byte(`{"name":"jdbc-sink","config":{"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector","connection.url":"jdbc:mysql://h:3306/db","table.name.format":"orders"}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "sql_insert:")
	assert.Contains(t, y, "driver: mysql")
	assert.Contains(t, y, "table: orders")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/connect_converter/ -run 'TestJDBC|TestConvertJDBC' -v`
Expected: FAIL — `undefined: jdbcDriver` / fallback stub.

- [ ] **Step 3: Implement `conn_jdbc.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import "strings"

func init() {
	registerConnector("io.confluent.connect.jdbc.JdbcSourceConnector", jdbcSourceConnector{})
	registerConnector("io.confluent.connect.jdbc.JdbcSinkConnector", jdbcSinkConnector{})
}

// jdbcDriver maps a JDBC URL to the RPCN sql driver name.
func jdbcDriver(url string) string {
	switch {
	case strings.HasPrefix(url, "jdbc:postgresql:"):
		return "postgres"
	case strings.HasPrefix(url, "jdbc:mysql:"):
		return "mysql"
	case strings.HasPrefix(url, "jdbc:sqlserver:"):
		return "mssql"
	case strings.HasPrefix(url, "jdbc:clickhouse:"):
		return "clickhouse"
	default:
		return ""
	}
}

// dsnFromURL strips the leading "jdbc:" so the remainder can be used as a DSN.
func dsnFromURL(url string) string {
	return strings.TrimPrefix(url, "jdbc:")
}

func driverAndDSN(ctx *MapCtx, body *yaml.Node) {
	url, ok := ctx.String("connection.url")
	if !ok {
		ctx.Warn("connection.url", "missing JDBC connection URL")
		stub := scalar("")
		stub.LineComment = "TODO: set driver and dsn"
		kv(body, "driver", stub)
		return
	}
	driver := jdbcDriver(url)
	dn := scalar(driver)
	if driver == "" {
		dn.LineComment = "TODO: unrecognized JDBC URL — set the driver manually"
	}
	kv(body, "driver", dn)
	dsn := scalar(dsnFromURL(url))
	dsn.LineComment = "TODO: verify DSN format for the chosen driver"
	kv(body, "dsn", dsn)
}

type jdbcSourceConnector struct{}

func (jdbcSourceConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()
	driverAndDSN(ctx, body)

	if v, ok := ctx.String("table.whitelist"); ok {
		kv(body, "table", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the source table (or use a query)"
		kv(body, "table", stub)
	}
	cols := scalar("*")
	cols.LineComment = "TODO: list specific columns if needed"
	kv(body, "columns", seq(cols))

	return Component{Input: component("sql_select", body)}, nil
}

type jdbcSinkConnector struct{}

func (jdbcSinkConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()
	driverAndDSN(ctx, body)

	if v, ok := ctx.String("table.name.format"); ok {
		kv(body, "table", scalar(v))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set the destination table"
		kv(body, "table", stub)
	}
	cols := scalar("")
	cols.LineComment = "TODO: list destination columns matching your message fields"
	kv(body, "columns", seq(cols))
	args := scalar(`root = [ this.id ]`)
	args.LineComment = "TODO: map message fields to column values (args_mapping)"
	kv(body, "args_mapping", args)

	return Component{Output: component("sql_insert", body)}, nil
}
```

Add `"gopkg.in/yaml.v3"` to the import block (used by `driverAndDSN`'s `*yaml.Node` param).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/connect_converter/ -run 'TestJDBC|TestConvertJDBC' -v`
Expected: PASS. If `sql_select`/`sql_insert` reject a field, check field names in `internal/impl/sql/` and adjust.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/conn_jdbc.go internal/connect_converter/conn_jdbc_test.go
git commit -m "feat(connect_converter): JDBC source and sink connector mappers"
```

---

### Task 11: MirrorMaker source connector mapper (both sides)

**Files:**
- Create: `internal/connect_converter/conn_mirror.go`
- Test: `internal/connect_converter/conn_mirror_test.go`

**Interfaces:**
- Produces: registration of `org.apache.kafka.connect.mirror.MirrorSourceConnector` → both a `kafka_franz` input and a `redpanda` output (sets `Component.Input` AND `Component.Output`).

KC → RPCN: `source.cluster.bootstrap.servers`→ input `seed_brokers`; `topics`→ input `topics`; `target.cluster.bootstrap.servers`→ output `seed_brokers`; output `topic` uses `${! @kafka_topic }`.

- [ ] **Step 1: Write failing test in `conn_mirror_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertMirror(t *testing.T) {
	in := []byte(`{"name":"mm","config":{
	  "connector.class":"org.apache.kafka.connect.mirror.MirrorSourceConnector",
	  "source.cluster.bootstrap.servers":"src:9092",
	  "target.cluster.bootstrap.servers":"dst:9092",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "kafka_franz:")
	assert.Contains(t, y, "src:9092")
	assert.Contains(t, y, "dst:9092")
	// both sides populated — no TODO input stub.
	assert.NotContains(t, y, "TODO: set the input")
	assert.NotContains(t, y, "TODO: set the output")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/connect_converter/ -run TestConvertMirror -v`
Expected: FAIL — fallback stub.

- [ ] **Step 3: Implement `conn_mirror.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import "strings"

func init() {
	registerConnector("org.apache.kafka.connect.mirror.MirrorSourceConnector", mirrorSourceConnector{})
}

type mirrorSourceConnector struct{}

func (mirrorSourceConnector) Map(cfg ConnectConfig, ctx *MapCtx) (Component, error) {
	in := mapping()
	if v, ok := ctx.String("source.cluster.bootstrap.servers"); ok {
		kv(in, "seed_brokers", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set source cluster brokers"
		kv(in, "seed_brokers", seq(stub))
	}
	if v, ok := ctx.String("topics"); ok {
		kv(in, "topics", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set topics to mirror"
		kv(in, "topics", seq(stub))
	}
	cg := scalar(cfg.Name + "-rpcn")
	cg.LineComment = "TODO: confirm consumer group name"
	kv(in, "consumer_group", cg)

	out := mapping()
	if v, ok := ctx.String("target.cluster.bootstrap.servers"); ok {
		kv(out, "seed_brokers", seq(scalarsFromCSV(v)...))
	} else {
		stub := scalar("")
		stub.LineComment = "TODO: set target cluster brokers"
		kv(out, "seed_brokers", seq(stub))
	}
	kv(out, "topic", scalar(`${! @kafka_topic }`))

	return Component{
		Input:  component("kafka_franz", in),
		Output: component("kafka_franz", out),
	}, nil
}

// scalarsFromCSV splits a comma-separated Kafka Connect value into scalar nodes.
func scalarsFromCSV(v string) []*yaml.Node {
	parts := strings.Split(v, ",")
	out := make([]*yaml.Node, 0, len(parts))
	for _, p := range parts {
		out = append(out, scalar(strings.TrimSpace(p)))
	}
	return out
}
```

Add `"gopkg.in/yaml.v3"` to the import block (used by `scalarsFromCSV`).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/connect_converter/ -run TestConvertMirror -v`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/conn_mirror.go internal/connect_converter/conn_mirror_test.go
git commit -m "feat(connect_converter): MirrorMaker source connector mapper"
```

---

### Task 12: Converters → deserialization processors

**Files:**
- Create: `internal/connect_converter/conv_serializers.go`
- Modify: `internal/connect_converter/convert.go` (replace the stub `mapConverters`)
- Test: `internal/connect_converter/conv_serializers_test.go`

**Interfaces:**
- Consumes: `ConverterMapper`, `registerConverter`, `lookupConverter`, `ConverterRole`.
- Produces: registration of `io.confluent.connect.avro.AvroConverter`, `org.apache.kafka.connect.json.JsonConverter`, `org.apache.kafka.connect.storage.StringConverter`, `io.confluent.connect.protobuf.ProtobufConverter`. Real `mapConverters(ctx)` implementation.

Semantics (v1, best-effort): converters describe the inbound wire format, so Avro/Protobuf emit a decode processor; JSON/String emit nothing (already structured/text). Only `value.converter` is mapped to a processor (key conversion rarely needs a pipeline step); `key.converter` presence is consumed silently.

- [ ] **Step 1: Write failing tests in `conv_serializers_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertWithAvroValueConverter(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "value.converter":"io.confluent.connect.avro.AvroConverter",
	  "value.converter.schema.registry.url":"http://sr:8081",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "processors:")
	assert.Contains(t, y, "schema_registry_decode:")
	assert.Contains(t, y, "url: http://sr:8081")
}

func TestConvertWithJSONValueConverter_NoProcessor(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "value.converter":"org.apache.kafka.connect.json.JsonConverter",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	assertValidRPCN(t, res.YAML)
	assert.NotContains(t, string(res.YAML), "schema_registry_decode")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/connect_converter/ -run TestConvertWith -v`
Expected: FAIL — no `schema_registry_decode` (stub returns nil).

- [ ] **Step 3: Implement `conv_serializers.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import "gopkg.in/yaml.v3"

func init() {
	registerConverter("io.confluent.connect.avro.AvroConverter", schemaRegistryConverter{})
	registerConverter("io.confluent.connect.protobuf.ProtobufConverter", schemaRegistryConverter{})
	registerConverter("org.apache.kafka.connect.json.JsonConverter", noopConverter{})
	registerConverter("org.apache.kafka.connect.storage.StringConverter", noopConverter{})
}

// schemaRegistryConverter emits a schema_registry_decode processor.
type schemaRegistryConverter struct{}

func (schemaRegistryConverter) Map(role ConverterRole, ctx *MapCtx) ([]*yaml.Node, error) {
	body := mapping()
	urlKey := role.Prefix() + ".schema.registry.url"
	if v, ok := ctx.String(urlKey); ok {
		kv(body, "url", scalar(v))
	} else {
		stub := scalar("http://localhost:8081")
		stub.LineComment = "TODO: set the schema registry URL"
		kv(body, "url", stub)
	}
	return []*yaml.Node{component("schema_registry_decode", body)}, nil
}

// noopConverter emits no processor (data is already structured/text).
type noopConverter struct{}

func (noopConverter) Map(role ConverterRole, ctx *MapCtx) ([]*yaml.Node, error) {
	return nil, nil
}
```

- [ ] **Step 4: Replace `mapConverters` in `convert.go`**

Replace the stub function body:

```go
// mapConverters maps the value converter to deserialization processors. The key
// converter's presence is consumed silently (rarely needs a pipeline step).
func mapConverters(ctx *MapCtx) []*yaml.Node {
	if cls, ok := ctx.String(KeyConverter.Prefix()); ok {
		_ = cls // consumed; no processor emitted for key conversion in v1
	}
	cls, ok := ctx.String(ValueConverter.Prefix())
	if !ok {
		return nil
	}
	m, found := lookupConverter(cls)
	if !found {
		ctx.Warn(ValueConverter.Prefix(), "unsupported value converter "+cls)
		return nil
	}
	nodes, err := m.Map(ValueConverter, ctx)
	if err != nil {
		ctx.Warn(ValueConverter.Prefix(), err.Error())
		return nil
	}
	return nodes
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/connect_converter/ -run TestConvertWith -v`
Expected: PASS. If `schema_registry_decode` rejects `url`, confirm the field name in `internal/impl/confluent/` and adjust.

- [ ] **Step 6: Format and commit**

```bash
task fmt
git add internal/connect_converter/conv_serializers.go internal/connect_converter/convert.go internal/connect_converter/conv_serializers_test.go
git commit -m "feat(connect_converter): converter mappers (avro/protobuf/json/string)"
```

---

### Task 13: SMTs → ordered processors

**Files:**
- Create: `internal/connect_converter/smt_insertfield.go`
- Create: `internal/connect_converter/smt_replacefield.go`
- Create: `internal/connect_converter/smt_regexrouter.go`
- Modify: `internal/connect_converter/convert.go` (replace the stub `mapSMTs`)
- Test: `internal/connect_converter/smt_test.go`

**Interfaces:**
- Consumes: `SMTMapper`, `registerSMT`, `lookupSMT`, `SMTConfig`.
- Produces: registration of three SMT types; real `mapSMTs(ctx)` that parses the `transforms` list in declared order, builds an `SMTConfig` per alias (stripping the `transforms.<alias>.` prefix), and dispatches.

- [ ] **Step 1: Write failing tests in `smt_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSMTInsertFieldStatic(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "transforms":"ins",
	  "transforms.ins.type":"org.apache.kafka.connect.transforms.InsertField$Value",
	  "transforms.ins.static.field":"src",
	  "transforms.ins.static.value":"kafka",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "mapping:")
	assert.Contains(t, y, `root.src = "kafka"`)
}

func TestSMTRegexRouter(t *testing.T) {
	in := []byte(`{"name":"s3","config":{
	  "connector.class":"io.confluent.connect.s3.S3SinkConnector",
	  "s3.bucket.name":"b",
	  "transforms":"r",
	  "transforms.r.type":"org.apache.kafka.connect.transforms.RegexRouter",
	  "transforms.r.regex":"(.*)",
	  "transforms.r.replacement":"prefix_$1",
	  "topics":"orders"
	}}`)
	res, err := Convert(in)
	require.NoError(t, err)
	y := string(res.YAML)
	assertValidRPCN(t, res.YAML)
	assert.Contains(t, y, "re_replace_all")
}

func TestSMTOrderPreserved(t *testing.T) {
	c := newTestCtx(map[string]any{
		"transforms":           "a,b",
		"transforms.a.type":    "org.apache.kafka.connect.transforms.RegexRouter",
		"transforms.a.regex":   "x",
		"transforms.a.replacement": "y",
		"transforms.b.type":    "org.apache.kafka.connect.transforms.InsertField$Value",
		"transforms.b.static.field": "f",
		"transforms.b.static.value": "v",
	})
	nodes := mapSMTs(c)
	require.Len(t, nodes, 2)
	// first node is the regex router (alias "a"), second is insertfield (alias "b").
	assert.Equal(t, "mapping", nodes[0].Content[0].Value)
	assert.Equal(t, "mapping", nodes[1].Content[0].Value)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/connect_converter/ -run TestSMT -v`
Expected: FAIL — stub `mapSMTs` returns nil.

- [ ] **Step 3: Implement `smt_insertfield.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func init() {
	registerSMT("org.apache.kafka.connect.transforms.InsertField$Value", insertFieldSMT{})
	registerSMT("org.apache.kafka.connect.transforms.InsertField$Key", insertFieldSMT{})
}

type insertFieldSMT struct{}

func (insertFieldSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	field, _ := smt.Props["static.field"].(string)
	value, _ := smt.Props["static.value"].(string)
	body := mapping()
	if field != "" {
		kv(body, "mapping", scalar(fmt.Sprintf("root.%s = %q", field, value)))
	} else {
		m := scalar("root = this")
		m.LineComment = "TODO: InsertField without static.field — map manually (timestamp/topic/etc.)"
		kv(body, "mapping", m)
	}
	return []*yaml.Node{component("mapping", body)}, nil
}
```

- [ ] **Step 4: Implement `smt_replacefield.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

func init() {
	registerSMT("org.apache.kafka.connect.transforms.ReplaceField$Value", replaceFieldSMT{})
	registerSMT("org.apache.kafka.connect.transforms.ReplaceField$Key", replaceFieldSMT{})
}

type replaceFieldSMT struct{}

func (replaceFieldSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	var lines []string
	if renames, ok := smt.Props["renames"].(string); ok && renames != "" {
		for _, pair := range strings.Split(renames, ",") {
			kvp := strings.SplitN(strings.TrimSpace(pair), ":", 2)
			if len(kvp) == 2 {
				lines = append(lines, fmt.Sprintf("root.%s = this.%s", kvp[1], kvp[0]))
				lines = append(lines, fmt.Sprintf("root.%s = deleted()", kvp[0]))
			}
		}
	}
	if exclude, ok := smt.Props["exclude"].(string); ok && exclude != "" {
		for _, f := range strings.Split(exclude, ",") {
			lines = append(lines, fmt.Sprintf("root.%s = deleted()", strings.TrimSpace(f)))
		}
	}
	body := mapping()
	if len(lines) == 0 {
		m := scalar("root = this")
		m.LineComment = "TODO: ReplaceField with include/whitelist semantics — map manually"
		kv(body, "mapping", m)
	} else {
		kv(body, "mapping", scalar(strings.Join(lines, "\n")))
	}
	return []*yaml.Node{component("mapping", body)}, nil
}
```

- [ ] **Step 5: Implement `smt_regexrouter.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func init() {
	registerSMT("org.apache.kafka.connect.transforms.RegexRouter", regexRouterSMT{})
}

type regexRouterSMT struct{}

func (regexRouterSMT) Map(smt SMTConfig, ctx *MapCtx) ([]*yaml.Node, error) {
	regex, _ := smt.Props["regex"].(string)
	replacement, _ := smt.Props["replacement"].(string)
	// Kafka Connect uses $1 backrefs; Go's re_replace_all uses $1 too.
	body := mapping()
	expr := fmt.Sprintf(`meta kafka_topic = metadata("kafka_topic").re_replace_all(%q, %q)`, regex, replacement)
	kv(body, "mapping", scalar(expr))
	return []*yaml.Node{component("mapping", body)}, nil
}
```

- [ ] **Step 6: Replace `mapSMTs` in `convert.go`**

```go
// mapSMTs parses the transforms list in declared order and maps each SMT to
// processor nodes.
func mapSMTs(ctx *MapCtx) []*yaml.Node {
	list, ok := ctx.String("transforms")
	if !ok || strings.TrimSpace(list) == "" {
		return nil
	}
	ctx.consume("transforms")

	var out []*yaml.Node
	for _, alias := range strings.Split(list, ",") {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		prefix := "transforms." + alias + "."
		typ, _ := ctx.String(prefix + "type")

		// Collect this SMT's sub-properties, stripping the prefix.
		props := map[string]any{}
		for k, v := range ctx.cfg.Props {
			if strings.HasPrefix(k, prefix) && k != prefix+"type" {
				ctx.consume(k)
				props[strings.TrimPrefix(k, prefix)] = v
			}
		}

		smt := SMTConfig{Alias: alias, Type: typ, Props: props}
		m, found := lookupSMT(typ)
		if !found {
			ctx.Warn(prefix+"type", "unsupported SMT "+typ)
			stub := mapping()
			s := scalar("root = this")
			s.LineComment = "TODO: unsupported SMT " + typ + " — map manually"
			kv(stub, "mapping", s)
			out = append(out, component("mapping", stub))
			continue
		}
		nodes, err := m.Map(smt, ctx)
		if err != nil {
			ctx.Warn(prefix, err.Error())
			continue
		}
		out = append(out, nodes...)
	}
	return out
}
```

Add `"strings"` to the `convert.go` import block.

- [ ] **Step 7: Run tests to verify they pass**

Run: `go test ./internal/connect_converter/ -run TestSMT -v`
Expected: PASS (all three).

- [ ] **Step 8: Run the full package test suite**

Run: `go test ./internal/connect_converter/ -v`
Expected: PASS (all tasks).

- [ ] **Step 9: Format and commit**

```bash
task fmt
git add internal/connect_converter/smt_insertfield.go internal/connect_converter/smt_replacefield.go internal/connect_converter/smt_regexrouter.go internal/connect_converter/convert.go internal/connect_converter/smt_test.go
git commit -m "feat(connect_converter): SMT mappers (InsertField/ReplaceField/RegexRouter)"
```

---

### Task 14: Golden-file test harness

**Files:**
- Create: `internal/connect_converter/golden_test.go`
- Create: `internal/connect_converter/testdata/*.input.json` and `*.expected.yaml` for: `s3_sink`, `gcs_sink`, `bigquery_sink`, `snowflake_sink`, `jdbc_source`, `jdbc_sink`, `mirror`, `avro_s3`, `smt_chain`.

**Interfaces:**
- Consumes: `Convert`.
- Produces: a table-driven golden test with an `-update` flag to regenerate fixtures.

- [ ] **Step 1: Write the golden harness in `golden_test.go`**

```go
// <PASTE LICENSE HEADER>

package connectconverter

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update golden files")

func TestGolden(t *testing.T) {
	cases := []string{
		"s3_sink", "gcs_sink", "bigquery_sink", "snowflake_sink",
		"jdbc_source", "jdbc_sink", "mirror", "avro_s3", "smt_chain",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			in, err := os.ReadFile(filepath.Join("testdata", name+".input.json"))
			require.NoError(t, err)

			res, err := Convert(in)
			require.NoError(t, err)
			assertValidRPCN(t, res.YAML)

			goldenPath := filepath.Join("testdata", name+".expected.yaml")
			if *update {
				require.NoError(t, os.WriteFile(goldenPath, res.YAML, 0o644))
				return
			}
			want, err := os.ReadFile(goldenPath)
			require.NoError(t, err)
			require.Equal(t, string(want), string(res.YAML))
		})
	}
}
```

- [ ] **Step 2: Create the `*.input.json` fixtures**

Create one `testdata/<name>.input.json` per case using the configs from Tasks 6–13 (flat or REST-wrapped JSON). For `avro_s3` use the Avro converter config from Task 12; for `smt_chain` use a config with `transforms:"r,ins"` combining RegexRouter and InsertField.

- [ ] **Step 3: Generate the golden outputs**

Run: `go test ./internal/connect_converter/ -run TestGolden -update`
Expected: creates `*.expected.yaml`. **Manually read each generated YAML** and confirm it looks correct (header present, fields right, TODO markers sensible). Fix any mapper bug and regenerate before committing.

- [ ] **Step 4: Run the golden test without update**

Run: `go test ./internal/connect_converter/ -run TestGolden -v`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
task fmt
git add internal/connect_converter/golden_test.go internal/connect_converter/testdata/
git commit -m "test(connect_converter): golden-file conversion fixtures"
```

---

### Task 15: CLI `convert` subcommand

**Files:**
- Create: `internal/cli/convert.go`
- Modify: `internal/cli/enterprise.go` (register `convertCli()` in the custom subcommands block, ~line 242)
- Test: `internal/cli/convert_test.go`

**Interfaces:**
- Consumes: `connectconverter.Convert`, `github.com/urfave/cli/v2`.
- Produces: `func convertCli() *cli.Command`.

CLI behavior: read the connector config from a path argument or stdin; write RPCN YAML to `-o`/`--output` file or stdout; print a one-line warning summary to stderr; exit 0 even with warnings; exit non-zero only on hard error (bad JSON / missing class / I/O).

- [ ] **Step 1: Write failing test in `convert_test.go`**

```go
// <PASTE LICENSE HEADER>

package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertCliBuilds(t *testing.T) {
	cmd := convertCli()
	require.NotNil(t, cmd)
	assert.Equal(t, "convert", cmd.Name)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cli/ -run TestConvertCliBuilds -v`
Expected: FAIL — `undefined: convertCli`.

- [ ] **Step 3: Implement `internal/cli/convert.go`**

```go
// <PASTE LICENSE HEADER>

package cli

import (
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"

	connectconverter "github.com/redpanda-data/connect/v4/internal/connect_converter"
)

func convertCli() *cli.Command {
	return &cli.Command{
		Name:      "convert",
		Usage:     "Convert a Kafka Connect connector config into a Redpanda Connect config",
		ArgsUsage: "[path]",
		Description: `
Reads a Kafka Connect connector configuration (REST-wrapped or flat JSON) from a
file argument or stdin and prints an equivalent Redpanda Connect pipeline YAML.
Sections that could not be fully mapped are annotated with # TODO markers.

  {{.BinaryName}} convert ./s3-sink.json -o ./s3-sink.yaml
  cat ./connector.json | {{.BinaryName}} convert`[1:],
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Write the converted config to a file instead of stdout.",
			},
		},
		Action: func(c *cli.Context) error {
			var (
				input []byte
				err   error
			)
			if path := c.Args().First(); path != "" {
				if input, err = os.ReadFile(path); err != nil {
					return fmt.Errorf("read input: %w", err)
				}
			} else {
				if input, err = io.ReadAll(c.App.Reader); err != nil {
					return fmt.Errorf("read stdin: %w", err)
				}
			}

			res, err := connectconverter.Convert(input)
			if err != nil {
				return err
			}

			if out := c.String("output"); out != "" {
				if err := os.WriteFile(out, res.YAML, 0o644); err != nil {
					return fmt.Errorf("write output: %w", err)
				}
			} else {
				if _, err := c.App.Writer.Write(res.YAML); err != nil {
					return err
				}
			}

			if len(res.Warnings) > 0 {
				fmt.Fprintf(c.App.ErrWriter, "conversion completed with %d warning(s); review # TODO markers\n", len(res.Warnings))
			}
			return nil
		},
	}
}
```

- [ ] **Step 4: Register the command in `enterprise.go`**

In the custom subcommands block (after `service.CLIOptAddCommand(pluginInit()),`), add:

```go
		service.CLIOptAddCommand(convertCli()),
```

- [ ] **Step 5: Run the CLI test and build**

Run: `go test ./internal/cli/ -run TestConvertCliBuilds -v`
Expected: PASS.

Run: `go build ./cmd/redpanda-connect/`
Expected: builds clean.

- [ ] **Step 6: Manual smoke test**

```bash
echo '{"name":"s3","config":{"connector.class":"io.confluent.connect.s3.S3SinkConnector","s3.bucket.name":"b","topics":"orders"}}' | go run ./cmd/redpanda-connect convert
```
Expected: prints RPCN YAML with `aws_s3:` output and a TODO input stub; stderr shows a warning count.

- [ ] **Step 7: Format, lint, and commit**

```bash
task fmt
task lint
git add internal/cli/convert.go internal/cli/convert_test.go internal/cli/enterprise.go
git commit -m "feat(cli): add 'convert' subcommand for Kafka Connect configs"
```

---

## Self-Review

**Spec coverage:**
- Form (deterministic Go engine + thin CLI) → Tasks 1–15. ✓
- Engine in `internal/connect_converter/` → all engine tasks. ✓
- Input: REST-wrapped + flat JSON → Task 1. ✓
- Connectors: mirror (T11), Snowflake (T9), BigQuery (T8), S3 (T6), GCS (T7), JDBC source+sink (T10). ✓
- Converters: avro/json/string/protobuf, common subset → Task 12. ✓
- SMTs: InsertField/ReplaceField/RegexRouter, common subset → Task 13. ✓
- Output: YAML + inline `# TODO` + stderr summary → assemble/render (T5), CLI summary (T15). ✓
- Source vs sink side, SMT ordering after converters → assemble (T5), convert ordering (T5/T12/T13). ✓
- Unknown connector/SMT/converter + leftover fields → warning + TODO, never hard-fail → fallback (T4), unmapped sweep (T2/T5), SMT/converter misses (T12/T13). ✓
- Hard error only on malformed JSON / missing class → Task 1. ✓
- Round-trip validation via benthos linter → `assertValidRPCN` used in T5–T14. ✓
- Per-mapper unit tests + golden tests → each task + Task 14. ✓
- Provenance header → assemble (T5). ✓

**Placeholder scan:** No "TBD"/"implement later"/"add error handling" steps. The `<PASTE LICENSE HEADER>` markers are deliberate, concrete instructions (copy the verbatim header from a sibling file) — the engineer must paste the repo's actual license block, which differs per distribution and cannot be hardcoded here without risking a CI header mismatch.

**Type consistency:** `Component{Input, Output *yaml.Node}` used uniformly (T1, T4, all connectors). `MapCtx.String`/`consume`/`Unmapped`/`Warn` consistent (T2 → all). `ConverterMapper.Map(role, ctx)` matches usage in `mapConverters` (T12). `SMTMapper.Map(smt, ctx)` matches `mapSMTs` (T13). `component`/`mapping`/`kv`/`scalar`/`seq` signatures consistent (T3 → all). `Convert`/`parse`/`assemble`/`render` signatures consistent (T1/T5).

**Note for implementer:** RPCN field names for `snowflake_streaming`, `sql_select`/`sql_insert`, `gcp_bigquery`, and `schema_registry_decode` are best-effort in this plan. The `assertValidRPCN` linter call in each task is the guard: if a field name is wrong, the test fails with a lint error — check the component's spec in `internal/impl/<category>/` (or `rpk connect list`) and correct the field name. This is expected, normal iteration, not a plan defect.
