# CLAUDE.md - Internal Implementation Guide

Go code patterns, testing standards, and development workflow for Redpanda Connect internal implementations.

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
