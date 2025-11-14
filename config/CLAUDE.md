# CLAUDE.md - Configuration Guide

YAML configuration and Bloblang transformation language for Redpanda Connect.

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
