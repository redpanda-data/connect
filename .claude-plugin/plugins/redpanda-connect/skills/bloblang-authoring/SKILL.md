---
name: bloblang-authoring
description: This skill should be used when users need to create or debug Bloblang transformation scripts. Trigger when users ask about transforming data, mapping fields, parsing JSON/CSV/XML, converting timestamps, filtering arrays, or mention "bloblang", "blobl", "mapping processor", or describe any data transformation need like "convert this to that" or "transform my JSON".
---

# Redpanda Connect Bloblang Script Generator

Create working, tested Bloblang transformation scripts from natural language descriptions.

## Objective

Generate a Bloblang (blobl) script that correctly transforms the user's input data according to their requirements.
The script MUST be tested before presenting it.

## Setup

This skill requires `rpk` `rpk connect`, `python3`, and `jq`.
See the [SETUP](SETUP.md) for installation instructions.

## Tools

### Script format-bloblang.sh

Generates category-organized Bloblang reference files in XML format.
**Run once at the start of each session** before searching for functions/methods.

```bash
# Usage:
./resources/scripts/format-bloblang.sh
```
- No arguments
- Generates category files organized by type (e.g., `functions-General.xml`, `methods-String_Manipulation.xml`)
- Outputs generated files to a versioned directory
- Outputs the directory path to stdout (capture in `BLOBLREF_DIR` variable for later use)
- Each XML file contains structured function/method definitions with parameters, descriptions, and examples

#### Functions

Generated function files have `functions-<Category>.xml` names and contain functions relevant to that category.

- `functions-Encoding.xml` - Schema registry headers
- `functions-Environment.xml` - Environment vars, files, timestamps, hostname
- `functions-Fake_Data_Generation.xml` - Fake data generation
- `functions-General.xml` - Bytes, counter, deleted, ksuid, nanoid, uuid, random, range, snowflake
- `functions-Message_Info.xml` - Batch index, content, error, metadata, span links, tracing IDs
- etc.

**The `function` XML tag format:**
- `name` attribute - function name
- `params` attribute - comma-separated list of parameters with types, format `<name>:<type>` or empty string if no parameters
- body - description of function purpose and usage
- `example` XML subtag
  - `summary` attribute (optional) - brief description of the example
  - body - code block demonstrating usage

Example function definition:
```xml
<function name="random_int" params="seed:query expression, min:integer, max:integer">
Generates a pseudo-random non-negative 64-bit integer.
Use this for creating random IDs, sampling data, or generating test values.
Provide a seed for reproducible randomness, or use a dynamic seed like `timestamp_unix_nano()` for unique values per mapping instance.

Optional `min` and `max` parameters constrain the output range (both inclusive).
For dynamic ranges based on message data, use the modulo operator instead: `random_int() % dynamic_max + dynamic_min`.
<example>
root.first = random_int()
root.second = random_int(1)
root.third = random_int(max:20)
root.fourth = random_int(min:10, max:20)
root.fifth = random_int(timestamp_unix_nano(), 5, 20)
root.sixth = random_int(seed:timestamp_unix_nano(), max:20)
</example>
<example summary="Use a dynamic seed for unique random values per mapping instance.">
root.random_id = random_int(timestamp_unix_nano())
root.sample_percent = random_int(seed: timestamp_unix_nano(), min: 0, max: 100)
</example>
</function>
```

#### Methods

Generated method files have `methods-<Category>.xml` names and contain methods relevant to that category.

- `methods-Encoding_and_Encryption.xml` - Base64, compression, hashing, encryption
- `methods-General.xml` - Basic operations, type checking
- `methods-GeoIP.xml` - GeoIP lookups
- `methods-JSON_Web_Tokens.xml` - JWT operations
- `methods-Number_Manipulation.xml` - Arithmetic, rounding, formatting
- `methods-Object___Array_Manipulation.xml` - Filtering, mapping, sorting, merging
- `methods-Parsing.xml` - JSON, CSV, XML, protocol buffer parsing
- `methods-Regular_Expressions.xml` - Regex matching and replacement
- `methods-SQL.xml` - SQL operations
- `methods-String_Manipulation.xml` - Case, trimming, splitting, formatting
- `methods-Timestamp_Manipulation.xml` - Parsing, formatting, timezone conversion
- `methods-Type_Coercion.xml` - Type conversions
- etc.

**The `method` XML tag format:**
- `name` attribute - function name
- `params` attribute - comma-separated list of parameters with types, format `<name>:<type>` or empty string if no parameters
- body - description of function purpose and usage
- `example` XML subtag
  - `summary` attribute (optional) - brief description of the example
  - body - code block demonstrating usage

Example method definition:
```xml
<method name="ts_format" params="format:string, tz:string">
Formats a timestamp into a string using the specified format layout.
<example>
root.formatted = this.timestamp.ts_format("2006-01-02T15:04:05Z07:00")
</example>
</method>
```

### Grep Search

Lists Available functions and methods without loading full files.

```bash
# List all available functions and methods by name
grep -hE '<(function|method) name=' "$BLOBLREF_DIR"

# Search by keyword (searches names, descriptions, params, examples)
grep -i "timestamp" "$BLOBLREF_DIR"

# Search by parameter name (e.g., find all with "format" parameter)
grep 'params="[^"]*format' "$BLOBLREF_DIR"
```
- Requires `BLOBLREF_DIR` set to the directory output by `format-bloblang.sh`

### Script test-blobl.sh

Tests a Bloblang script against input data.
Executes the transformation and returns results or errors.
Can be run repeatedly during iteration.

```bash
# Usage:
./resources/scripts/test-blobl.sh <target-directory>
```
- Requires `data.json` (input) and `script.blobl` (transformation) in the target directory
- Returns transformed data or error messages

## Bloblang

**Bloblang** (blobl) is Redpanda Connect's native mapping language for transforming message data.
It's designed for readability and safely reshaping documents of any structure.

### Core Concepts

**Assignment**: Create new documents by assigning values to paths.
- `root` = the new document being created
- `this` = the input document being read

```bloblang
# Copy entire input
root = this

# Create specific fields
root.id = this.thing.id
root.type = "processed"

# In:  {"thing":{"id":"abc123"}}
# Out: {"id":"abc123","type":"processed"}
```

**Field Paths**: Use dot notation for nested fields. Use quotes for special characters:
```bloblang
root.user.name = this.customer.full_name
root."foo.bar".baz = this."field with spaces"
```

**Literals**: Numbers, booleans, strings, null, arrays, and objects:
```bloblang
root = {
  "count": 42,
  "active": true,
  "items": ["a", "b", "c"],
  "nested": {"key": "value"}
}
```

### Functions and Methods

**Functions** generate values (no target needed):
```bloblang
root.id = uuid_v4()
root.timestamp = now()
root.hostname = hostname()
```

**Methods** transform values (called on a target with `.`):
```bloblang
root.upper = this.name.uppercase()
root.formatted = this.date.ts_parse("2006-01-02").ts_format("Mon Jan 2")
root.sorted = this.items.sort()
```

Methods can be chained:
```bloblang
root.clean = this.text.trim().lowercase().replace_all("_", "-")
```

Methods require a target (called with `.`), while functions do not. 
Check the XML reference files to determine correct usage:

```bloblang
# Bad: floor() is a method, not a function
root.rounded = floor(this.value)  # Error: floor is not a function

# Good: Call floor() as a method on a value
root.rounded = this.value.floor()

# Bad: uuid_v4() is a function, not a method
root.id = this.uuid_v4()  # Error: uuid_v4 is not a method

# Good: Call uuid_v4() as a function
root.id = uuid_v4()
```

**Discovering Available Functions & Methods**

Bloblang provides hundreds of functions and methods organized into categories.
Start with these **foundational categories** that cover common use cases:
- `functions-General.xml` - Core utility functions (uuid_v4, timestamp, random, etc.)
- `functions-Message_Info.xml` - Message metadata access (hostname, env, content_type, etc.)
- `methods-General.xml` - Universal transformations (type conversions, existence checks, etc.)

For specialized needs, consult **domain-specific categories**: strings (uppercase, trim, regexp), timestamps (ts_parse, ts_format), arrays (map_each, filter), objects (keys, values), encoding (base64, json), and more.

**Discovery tools**:
- Run `format-bloblang.sh` to generate category-organized XML reference files in a versioned directory
- Use grep patterns to search function/method names, descriptions, parameters, and examples across categories
- Read specific category XML files for structured definitions with complete function signatures, parameter details, and usage examples

### Control Flow

**Conditionals** (if/else):
```bloblang
root.category = if this.score >= 80 {
  "high"
} else if this.score >= 50 {
  "medium"
} else {
  "low"
}
```

**Pattern Matching** (match):
```bloblang
root.sound = match this.animal {
  "cat" => "meow"
  "dog" => "woof"
  "cow" => "moo"
  _ => "unknown"  # Catch-all
}
```

**Coalescing** (try multiple paths with `|`):
```bloblang
# Use first non-null value from alternative fields
root.content = this.article.body | this.comment.text | "no content"

# Try different nested paths
root.id = this.data.(primary_id | secondary_id | backup_id)
```

Note: Use `|` for alternative field paths (missing fields), use `.catch()` for operation failures (parse errors, type mismatches).

### Common Operations

**Deletion**:
```bloblang
root = this
root.password = deleted()  # Remove field

# Or filter entire message
root = if this.spam { deleted() }
```

**Variables** (reuse values without adding to output):
```bloblang
let user_id = this.user.id
let enriched = this.user.name + " (" + $user_id + ")"

root.display_name = $enriched
root.user_id = $user_id
```

**IMPORTANT**: Variables must be declared at the top level, not inside `if`, `match`, or other blocks.

```bloblang
# Bad: Will cause "expected }" parse error
root.age = if this.birthdate != null {
  let parsed = this.birthdate.ts_parse("2006-01-02")  # let not allowed here!
  $parsed.ts_unix()
}

# Good: Declare variables at top level
let parsed = this.birthdate.ts_parse("2006-01-02").catch(null)
root.age = if $parsed != null {
  $parsed.ts_unix()
} else {
  null
}
```

**Named mappings**: (reusable scripts)
```bloblang
map extract_user {
  root.id = this.user_id
  root.name = this.full_name
  root.email = this.contact.email
}

root.customer = this.customer_data.apply("extract_user")
root.vendor = this.vendor_data.apply("extract_user")
```

**Error Handling** (provide fallback values):
```bloblang
# Catch errors from any point in the chain
root.count = this.items.length().catch(0)
root.parsed = this.data.parse_json().catch({})

# Catch missing/null values
root.name = this.user.name.or("anonymous")

# Multi-format parsing with catch chains
# Store value in variable for reliable access in catch fallbacks
let date_str = this.date
root.parsed = $date_str.ts_parse("2006-01-02").catch(
  $date_str.ts_parse("2006/01/02")
).catch(null)
```

**IMPORTANT**: When using `.catch()` with fallback expressions that reference `this.field`, store the field in a variable first.
Context references in catch chains can be unreliable:

```bloblang
# Risky: Context may not be preserved in catch
root.parsed = this.date.ts_parse("2006-01-02").catch(
  this.date.ts_parse("2006/01/02")  # this.date might not work here
)

# Safe: Store in variable first
let date_str = this.date
root.parsed = $date_str.ts_parse("2006-01-02").catch(
  $date_str.ts_parse("2006/01/02")  # variable reference is reliable
)
```

**Metadata**:
```bloblang
# Read metadata with @ or metadata()
root.topic = @kafka_topic
root.partition = @kafka_partition

# Set metadata
meta output_key = this.id
meta content_type = "application/json"
```

### Common Edge Case Patterns

**Safe field access with fallbacks**
```bloblang
# Bad: Will fail if user or name is missing
root.name = this.user.name

# Good: Provides fallback chain
root.name = this.user.name.or("anonymous")
root.name = this.(user.name | profile.display_name | "unknown")
```

**Safe collection operations**
```bloblang
# Bad: Will fail on empty array
root.first = this.items[0]

# Good: Handles empty arrays
root.first = if this.items.length() > 0 { this.items[0] } else { null }
root.first = this.items[0].catch(null)
```

**Safe parsing with error recovery**
```bloblang
# Bad: Will fail on invalid JSON
root.data = this.payload.parse_json()

# Good: Provides fallback on parse failure
root.data = this.payload.parse_json().catch({})
root.data = this.payload.parse_json().catch(this.payload)  # Keep original on failure
```

**Safe type coercion**
```bloblang
# Bad: Assumes field is already a string
root.id = this.user_id.uppercase()

# Good: Converts to string first
root.id = this.user_id.string().uppercase()
root.count = this.total.number().catch(0)
```

**IMPORTANT**: Arithmetic operations on null values fail silently.
Always check for null or use `.catch()` to provide fallbacks:

```bloblang
# Bad: Fails silently if price is null
root.total = this.price * this.quantity

# Good: Check for null before operations
root.total = if this.price != null && this.quantity != null {
  this.price * this.quantity
} else {
  null
}

# Also good: Use catch to handle null gracefully
root.total = (this.price * this.quantity).catch(null)
```

## Workflow

1. **Understand** - Analyze input structure, desired output, and required transformations
     - **Ambiguous requirements**: If transformation goal is unclear, ask clarifying questions before proceeding (e.g., "Should missing fields be omitted or set to null?", "How should arrays with mixed types be handled?")
     - **Missing sample data**: If user doesn't provide input example, request it explicitly - never proceed with assumptions
     - **Complex multistep transformations**: Break down into logical phases (parse → transform → filter → format) and confirm approach with user

2. **Discover** - Generate category files to versioned directory (capture `BLOBLREF_DIR` from script output), identify relevant categories, read specific category XML files to find actual Bloblang functions/methods (NEVER guess)

3. **Develop** - Write valid Bloblang syntax using discovered functions (root for output, this for input, chain methods, handle nulls)

4. **Validate** - Test script with sample input data, verify output matches expectations, iterate on errors until working
     - **Test edge cases**: Missing fields, null values, invalid formats, empty collections
     - **Iterate**: Fix syntax errors first (variable placement, method chains), then logic errors

5. **Deliver** - Write the working script and example input to files (`script.blobl`, `data.json`), present the tested output, document any assumptions

**Critical: Never present untested code. All scripts must be validated before showing to user.**
