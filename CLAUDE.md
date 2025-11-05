# CLAUDE.md

AI agent guidance for working with Redpanda Connect codebase.

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
task test:unit                    # Run unit tests only
task test:unit-race               # Run unit tests with race detection
task test:template                # Run template/Bloblang tests
task test:integration-package PKG=./internal/impl/kafka/...  # Run integration tests for specific package
```

Integration tests require Docker and are skipped by default.
Run them individually per component.

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

Component availability controlled by `public/bundle/` (distribution-specific imports), `public/schema/` (schema generation per distribution), and `internal/plugins/info.csv` (component metadata).

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

---

## Go Code Patterns

### Naming Conventions

**Function names** - Avoid repetition of package name, receiver type, or parameter types:
```go
// Good
package yamlconfig
func Parse(input string) (*Config, error)
func (c *Config) WriteTo(w io.Writer) (int64, error)

// Bad
func ParseYAMLConfig(input string) (*Config, error)
func (c *Config) WriteConfigTo(w io.Writer) (int64, error)
```

**Receiver names** - 1-2 letter abbreviation of type, consistent across methods:
```go
// Good
func (c *Client) Get() {}
func (c *Client) Set() {}

// Bad
func (this *Client) Get() {}
func (self *Client) Set() {}
```

**Initialisms** - Keep consistent case: `URL` or `url`, never `Url`:
```go
// Good: ServeHTTP, xmlHTTPRequest, appID
// Bad: ServeHttp, XmlHttpRequest, appId
```

### Error Handling

**Use sentinel errors or custom types**:
```go
// Good
var ErrDuplicate = errors.New("duplicate")

switch err := process(an); {
case errors.Is(err, ErrDuplicate):
    return fmt.Errorf("feed %q: %v", an, err)
}

// Bad
if regexp.MatchString(`duplicate`, err.Error()) {...}
```

**Add context without redundancy**:
```go
// Good
if err := os.Open("settings.txt"); err != nil {
    return fmt.Errorf("launch codes unavailable: %v", err)
}
// Output: launch codes unavailable: open settings.txt: no such file or directory

// Bad
return fmt.Errorf("could not open settings.txt: %v", err)
// Output: could not open settings.txt: open settings.txt: no such file or directory
```

**%v vs %w**:
- `%v`: simple annotation, hides structure (use at boundaries)
- `%w`: preserve error chain for programmatic inspection

**Place %w at end** for readable error chains:
```go
// Good
err3 := fmt.Errorf("err3: %w", err2)
fmt.Println(err3)  // err3: err2: err1

// Bad
err3 := fmt.Errorf("%w: err3", err2)
fmt.Println(err3)  // err1: err2: err3 (backwards)
```

**Error strings** - Lowercase, no punctuation (unless proper noun/acronym):
```go
// Good
fmt.Errorf("something bad")

// Bad
fmt.Errorf("Something bad")
```

**Error flow indentation** - Handle errors first, keep normal path unindented:
```go
// Good
if err != nil {
    // error handling
    return
}
// normal code

// Bad
if err != nil {
    // error handling
} else {
    // normal code
}
```

### Function Design

**Context placement** - Context first parameter, never in struct:
```go
// Good
func F(ctx context.Context, other args) {}

// Bad
type Config struct {
    ctx context.Context  // never do this
}
```

**Option structures** - For many parameters, use option struct as last parameter:
```go
// Good
type ReplicationOptions struct {
    Config *replicator.Config
    PrimaryRegions []string
    ReplicateExisting bool
}

func EnableReplication(ctx context.Context, opts ReplicationOptions) {}

// Bad
func EnableReplication(ctx context.Context, config *replicator.Config, 
    primaryRegions, readonlyRegions []string, replicateExisting, 
    overwritePolicies bool, replicationInterval time.Duration) {}
```

**In-band errors** - Return explicit error/bool, not special values:
```go
// Good
func Lookup(key string) (value string, ok bool)

// Bad
func Lookup(key string) string  // returns "" on error
```

### Receiver Type Selection

Use **pointer receiver** when:
- Method mutates receiver
- Receiver has `sync.Mutex` or similar
- Receiver is large struct/array
- Any element might be mutating
- Consistency with other methods

Use **value receiver** when:
- Small unchanging struct or basic type
- No mutation needed

```go
// Good - pointer for mutex
type Counter struct {
    mu sync.Mutex
    data map[string]int64
}
func (c *Counter) Increment(name string) {}  // pointer receiver

// Good - value for small immutable
func (t time.Time) Format(layout string) string {}
```

### Variable Declarations

**Prefer := for non-zero values, var for zero values**:
```go
// Good
i := 42
var coords Point

// Bad
var i = 42
var coords = Point{X: 0, Y: 0}
```

**Shadowing vs Stomping** - Use `=` to reassign in new scope:
```go
// Bad - shadowing bug
if *shortenDeadlines {
    ctx, cancel := context.WithTimeout(ctx, 3*time.Second)  // new ctx variable
    defer cancel()
}
// ctx here is still the original

// Good - stomping
var cancel func()
ctx, cancel = context.WithTimeout(ctx, 3*time.Second)  // reassigns ctx
defer cancel()
```

### Comments

**Comment sentences** - Full sentences starting with declaration name, ending with period:
```go
// Good
// Request represents a request to run a command.
type Request struct {}

// Encode writes the JSON encoding of req to w.
func Encode(w io.Writer, req *Request) {}
```

### Misc Patterns

**Empty slices** - Prefer `var t []string` over `t := []string{}`:
```go
// Good
var t []string  // nil slice

// Bad
t := []string{}  // non-nil but empty
```

**Import organization** - Standard library first, then blank line, then third-party:
```go
import (
    "fmt"
    "os"

    "github.com/foo/bar"
)
```

**Interfaces** - Define in consumer package, not producer:
```go
// Good - consumer defines interface
package consumer
type Thinger interface { Thing() bool }
func Foo(t Thinger) string {}

// Good - producer returns concrete type
package producer
type Thinger struct{}
func (t Thinger) Thing() bool {}
func NewThinger() Thinger {}
```

---

## Unit Testing Patterns

### Table-Driven Tests

Use slices for test cases with `t.Run()`:

```go
func TestFunctionName(t *testing.T) {
    tests := []struct {
        name  string
        input string
        sep   string
        want  []string
    }{
        {name: "simple_case", input: "a/b/c", sep: "/", want: []string{"a", "b", "c"}},
        {name: "edge_case", input: "abc", sep: "/", want: []string{"abc"}},
        {name: "empty_input", input: "", sep: "/", want: []string{""}},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got := FunctionName(tt.input, tt.sep)
            
            if diff := cmp.Diff(tt.want, got); diff != "" {
                t.Fatalf("mismatch (-want +got):\n%s", diff)
            }
        })
    }
}
```

**Rules**:
- Use `[]struct` with `name` field for test cases
- Always use `t.Run()` for subtests
- Use `cmp.Diff()` from `github.com/google/go-cmp/cmp` for comparisons
- Use `t.Fatalf()` inside subtests (not `t.Errorf()`)

### Testify: assert vs require

**Use `require` for critical preconditions**:
- Setup validation
- Input preconditions
- Mock expectations
- Any check where continuing would cause panic or noise

```go
require.NoError(t, err, "setup failed")
require.NotNil(t, db, "database connection required")
```

**Use `assert` for independent checks**:
- Multiple field validations
- Independent assertions in the same test
- Non-critical checks

```go
assert.Equal(t, expected.Name, actual.Name)
assert.Equal(t, expected.Age, actual.Age)
assert.True(t, actual.Active)
```

**Rule of thumb**:
```go
func TestUserCreation(t *testing.T) {
    // require for setup - must succeed
    db := setupDB(t)
    require.NotNil(t, db)
    
    user, err := CreateUser(db, "john")
    require.NoError(t, err) // must succeed to continue
    
    // assert for multiple independent checks
    assert.Equal(t, "john", user.Name)
    assert.NotEmpty(t, user.ID)
    assert.False(t, user.Deleted)
}
```

**Prefer specific assertions**:
```go
// Bad - unclear failure message
assert.True(t, a == b)

// Good - clear failure message
assert.Equal(t, a, b)

// Bad - string matching
assert.Contains(t, err.Error(), "not found")

// Good - error type checking
assert.ErrorIs(t, err, ErrNotFound)
```

### Error Testing

Split success and error cases into separate test functions:

```go
// Success cases
func TestFunctionName(t *testing.T) {
    tests := []struct {
        name  string
        input string
        want  Result
    }{
        {name: "valid_input", input: "valid", want: Result{Value: 42}},
        {name: "another_case", input: "test", want: Result{Value: 10}},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := FunctionName(tt.input)
            require.NoError(t, err)
            assert.Equal(t, tt.want, got)
        })
    }
}

// Error cases
func TestFunctionName_Errors(t *testing.T) {
    tests := []struct {
        name  string
        input string
    }{
        {name: "empty_input", input: ""},
        {name: "invalid_format", input: "bad"},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            _, err := FunctionName(tt.input)
            require.Error(t, err)
        })
    }
}
```

### Test Helpers

Use `t.Helper()` to mark helper functions:

```go
func setupDB(t *testing.T) *sql.DB {
    t.Helper()
    
    db, err := sql.Open("sqlite3", ":memory:")
    require.NoError(t, err)
    
    t.Cleanup(func() { db.Close() })
    return db
}
```

### Context in Tests

Use `t.Context()` to get a context that is canceled when the test completes:

```go
func TestFunctionName(t *testing.T) {
    ctx := t.Context()
    
    result, err := FunctionName(ctx, input)
    require.NoError(t, err)
    assert.Equal(t, expected, result)
}
```

**Why**: `t.Context()` is automatically canceled when the test ends, preventing goroutine leaks.

### Test Coverage

**Always test**:
- Empty inputs (`""`, `nil`, `[]`)
- Single element
- Edge cases (trailing/leading)
- Error conditions

---

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

---

## Integration Testing Patterns

Integration tests follow **Given-When-Then** structure with `t.Log()` to mark phases.

### Basic Structure

```go
func TestIntegrationFeature(t *testing.T) {
    integration.CheckSkip(t)
    
    t.Log("Given: initial system state")
    // Setup infrastructure
    
    t.Log("When: action is performed")
    // Execute operation
    
    t.Log("Then: expected outcome")
    // Verify results
    
    t.Log("And: additional verification")
    // More assertions
}
```

**Why**: Logs create narrative. Failures show exact phase. Don't repeat "Given" in subtests unless state differs.

### Async Operations

Background goroutines with cleanup:

```go
go func() {
    if err := stream.Run(t.Context()); err != nil && !errors.Is(err, context.Canceled) {
        t.Error(err)
    }
    t.Log("Pipeline shutdown")
}()

t.Cleanup(func() {
    require.NoError(t, stream.StopWithin(stopStreamTimeout))
})
```

**Why**: Ignore `context.Canceled` in background goroutines.

### Polling and Retries

```go
// assert.Eventually - log state on each attempt
assert.Eventually(t, func() bool {
    offsets, err := dst.Admin.FetchOffsets(t.Context(), group)
    require.NoError(t, err)
    t.Log(offsets)  // Shows progress
    return offsets[topic][0].At == 1000
}, timeout, interval)

// pool.Retry - create/close resources in retry function
require.NoError(t, pool.Retry(func() error {
    client, err := kgo.NewClient(kgo.SeedBrokers(brokerAddr))
    if err != nil {
        return err
    }
    defer client.Close()
    
    ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
    defer cancel()
    return client.Ping(ctx)
}))
t.Log("Container is healthy")
```

**Why**: Use `require` inside `assert.Eventually` (safe). Log after retry success.

### Cleanup Error Handling

Log cleanup errors without failing test:

```go
t.Cleanup(func() {
    if err := s.StopWithin(time.Second); err != nil {
        t.Log(err)
    }
})
```

### Metrics Verification

Return nil on error, log failures, don't fail test:

```go
func readMetrics(t *testing.T, baseURL string) map[string]any {
    t.Helper()
    
    resp, err := http.Get(baseURL + "/stats")
    if err != nil {
        t.Logf("Failed to fetch metrics: %v", err)
        return nil
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        t.Logf("Metrics endpoint returned status %d", resp.StatusCode)
        return nil
    }
    
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        t.Logf("Failed to read metrics response: %v", err)
        return nil
    }
    
    var metrics map[string]any
    if err := json.Unmarshal(body, &metrics); err != nil {
        t.Logf("Failed to parse metrics JSON: %v", err)
        return nil
    }
    return metrics
}

// Verify existence, log filtered subset
metrics := readMetrics(t, "http://"+httpAddr)
require.NotEmpty(t, metrics)

for key, value := range metrics {
    if strings.Contains(key, "redpanda") {
        t.Logf("  %s: %v", key, value)
    }
}
```

### Parallel Subtests with Shared Setup

Setup before subtests, subtests only read:

```go
func TestIntegrationListGroupOffsets(t *testing.T) {
    integration.CheckSkip(t)
    
    // Shared setup
    src, dst := startRedpandaSourceAndDestination(t)
    src.CreateTopic(topicFoo1)
    writeToTopic(src, 5, ProduceToTopicOpt(topicFoo1))
    
    // Parallel subtests - read-only operations
    t.Run("all groups", func(t *testing.T) {
        t.Parallel()
        offsets := listGroupOffsets(t, conf, []string{topicFoo1})
        assert.ElementsMatch(t, expected, offsets)
    })
    
    t.Run("include pattern", func(t *testing.T) {
        t.Parallel()
        offsets := listGroupOffsets(t, confWithFilter, []string{topicFoo1})
        assert.ElementsMatch(t, expectedFiltered, offsets)
    })
}
```

**Why**: No mutations in parallel subtests.

---

## YAML Configuration

Component categories:
- `inputs` - Data sources
- `outputs` - Data destinations
- `processors` - Data transformations
- `caches` - Key-value stores
- `buffers` - Message queuing
- `rate-limits` - Rate limiting
- `metrics` - Observability
- `tracers` - Distributed tracing
- `bloblang-functions` - Bloblang functions
- `bloblang-methods` - Bloblang methods

### Basic Pipeline Structure

```yaml
input:
  <input_type>:
    # input fields from JSON schema

pipeline:
  processors:
    - <processor_type>:
        # processor fields from JSON schema

output:
  <output_type>:
    # output fields from JSON schema
```

### With Resources (Reusable Components)

```yaml
input_resources:
  - label: my_input
    <input_type>:
      # configuration

processor_resources:
  - label: my_processor
    <processor_type>:
      # configuration

output_resources:
  - label: my_output
    <output_type>:
      # configuration

input:
  resource: my_input

pipeline:
  processors:
    - resource: my_processor

output:
  resource: my_output
```

---

## Bloblang

Bloblang is the primary data transformation language in Redpanda Connect.

### Discovering Bloblang

**List functions:**
```bash
go run ./cmd/redpanda-connect-cloud/ list bloblang-functions
```

**List methods:**
```bash
go run ./cmd/redpanda-connect-cloud/ list bloblang-methods
```

### Testing Bloblang Scripts

**IMPORTANT**: Input JSON must end with newline, otherwise `blobl` produces no output.

**Single-line mapping:**
```bash
# Using printf (recommended - ensures newline)
printf '{"foo":"bar"}\n' | go run ./cmd/redpanda-connect-cloud/ blobl 'root.foo = this.foo.uppercase()'

# Using echo (adds newline automatically)
echo '{"foo":"bar"}' | go run ./cmd/redpanda-connect-cloud/ blobl 'root.foo = this.foo.uppercase()'

# Using heredoc (adds newline automatically)
go run ./cmd/redpanda-connect-cloud/ blobl 'root.foo = this.foo.uppercase()' <<< '{"foo":"bar"}'
```

**Multi-line mappings:**
```bash
# Create mapping file
cat > test.blobl <<'EOF'
let decoded = this.bar.decode("hex")
let value = $decoded.number()
root = this.merge({"result": $value})
EOF

# Test it
printf '{"bar":"64"}\n' | go run ./cmd/redpanda-connect-cloud/ blobl -f test.blobl
```

### Bloblang Basics

**Assignment**: `root.field = expression`

**This context**: `this` refers to input document

**Metadata**: Access with `@` prefix or `meta()` function

**Variables**: Define with `let`, reference with `$`

```bloblang
let name = this.user.name
root.greeting = "Hello " + $name
```

**Conditionals**: `if/else` expressions

```bloblang
root.status = if this.age >= 18 { "adult" } else { "minor" }
```

**Deletion**: Use `deleted()` to remove fields

```bloblang
root = this
root.sensitive_field = deleted()
```

### Common Bloblang Patterns

**Copy and modify**:
```bloblang
root = this
root.timestamp = now()
root.id = uuid_v4()
```

**Conditional field**:
```bloblang
root = this
root.category = if this.score > 80 { "high" } else { "low" }
```

**Array transformation**:
```bloblang
root.items = this.items.map_each(item -> item.uppercase())
root.filtered = this.items.filter(item -> item.score > 50)
```

**Array folding (reduce)**:
```bloblang
# Sum array values
let sum = this.numbers.fold(0, item -> item.tally + item.value)

# Product of array values
let product = this.numbers.fold(1, item -> item.tally * item.value)

# Convert byte array to integer (big-endian)
let int_value = range(0, this.bytes.length()).fold(0, item -> item.tally * 256 + this.bytes.index(item.value))

# Note: item.tally is the accumulator, item.value is the current element
```

**Metadata access**:
```bloblang
root = this
root.topic = @kafka_topic
root.partition = @kafka_partition
root.custom = meta("custom_key")
```

**Error handling**:
```bloblang
root.value = this.field.catch("default")
root.parsed = this.json_field.parse_json().catch({})
```

### Bloblang Writing Workflow

1. List available functions and methods
2. Write bloblang script
3. Test bloblang script with JSON data using `printf '{"data":"value"}\n' | go run ./cmd/redpanda-connect-cloud/ blobl -f script.blobl`
4. Iterate, if more information is needed read the function implementation

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
