---
name: rpcn:blobl
description: Create and test Bloblang transformation scripts from natural language descriptions
arguments:
  - name: description
    description: Natural language description of the transformation (e.g., "Convert timestamp to ISO format and uppercase the name field")
    required: true
  - name: sample_input
    description: Optional JSON sample input for testing
    required: false
---

# Redpanda Connect Bloblang Script Generator

Create working, tested Bloblang transformation scripts from natural language descriptions.

## Objective

Generate a Bloblang script that correctly transforms the user's input data according to their requirements. The script must be tested and validated before presenting it.

## Context

**Bloblang** is Redpanda Connect's transformation language for manipulating message data. It provides:
- **Functions** - Generate values (e.g., `uuid_v4()`, `now()`, `counter()`)
- **Methods** - Transform values (e.g., `.uppercase()`, `.ts_parse()`, `.filter()`)
- **Syntax** - Mappings start with `root`, reference input with `this`

**Available Resources:**

**format-bloblang.sh** - Generates category-organized Bloblang reference files
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/format-bloblang.sh --output-dir <directory>

# Example:
${CLAUDE_PLUGIN_ROOT}/scripts/format-bloblang.sh --output-dir /tmp/my-session
```
- Splits Bloblang functions/methods into ~16 category files
- Categories include: General, String_Manipulation, Timestamp_Manipulation, Object_&_Array_Manipulation, etc.
- Each file contains XML-formatted function/method definitions with parameters and examples
- Output directory contains category files like `functions-General.txt`, `methods-String_Manipulation.txt`, etc.

**test-blobl.sh** - Tests a Bloblang script against input data
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/test-blobl.sh <session-directory>

# Example:
${CLAUDE_PLUGIN_ROOT}/scripts/test-blobl.sh /tmp/my-session
```
- Expects `data.json` (input) and `script.blobl` (transformation) in the session directory
- Executes the Bloblang script and returns results or errors
- Can be run repeatedly during iteration

**Session Pattern:**

Create a temporary session directory to hold all working files:
```bash
# Create temp directory (important: save the path for passing to scripts)
SESSION_DIR=$(mktemp -d -t rpcn-blobl-XXXXXX)
# Creates: /tmp/rpcn-blobl-XXXXXX/ (or system equivalent)
```

The session directory contains:
- Category reference files (generated once at start by format-bloblang.sh)
- `data.json` - Sample input for testing (immutable during iteration)
- `script.blobl` - The transformation script (edit and re-test as needed)

Clean up when done:
```bash
rm -rf "$SESSION_DIR"
```

## Requirements

1. **Understand the transformation** - Identify input structure, desired output, and operations needed

2. **Discover relevant functions** - Find the right Bloblang functions/methods to use:
   - Generate category files into a session directory
   - Identify which categories are relevant (strings, timestamps, arrays, etc.)
   - Read only the relevant category files to find appropriate functions/methods
   - Category files contain full details: names, parameters, descriptions, examples

3. **Generate the script** - Write valid Bloblang syntax:
   - Use `root` for output fields
   - Use `this` for input fields
   - Chain methods for readability
   - Handle null/missing fields appropriately

4. **Test thoroughly** - Validate the script works:
   - Create or use provided sample input
   - Run the script against the input data
   - Verify the output matches expectations
   - If errors occur, debug and iterate until it works

5. **Present complete results** - Provide:
   - The working script with comments
   - Sample input and actual output
   - Usage examples (in YAML config or standalone)
   - Any assumptions about input structure

## Tool Interactions

**Creating temporary directories**:
Use `mktemp` to create temporary directories that can be passed to scripts:
```bash
SESSION_DIR=$(mktemp -d -t rpcn-blobl-XXXXXX)
# The variable $SESSION_DIR now contains the path (e.g., /tmp/rpcn-blobl-abc123)
# This path is passed to format-bloblang.sh and test-blobl.sh
```

**format-bloblang.sh**:
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/format-bloblang.sh --output-dir <directory>
```
- Accepts `--output-dir` parameter pointing to where category files should be written
- Generates category files organized by function type
- Files are XML-formatted with structured function/method definitions

**Generated Category Files:**

The three foundational categories (`functions-General.txt`, `functions-Message_Info.txt`, `methods-General.txt`) contain universally useful functions/methods that apply to most transformations. When discovering functions for a transformation, these should typically be consulted along with any specialized categories relevant to the specific transformation needs (strings, timestamps, arrays, etc.).

Function categories:
- `functions-Encoding.txt` - Schema registry headers
- `functions-Environment.txt` - Environment vars, files, timestamps, hostname
- `functions-Fake_Data_Generation.txt` - Fake data generation
- `functions-General.txt` - Bytes, counter, deleted, ksuid, nanoid, uuid, random, range, snowflake
- `functions-Message_Info.txt` - Batch index, content, error, metadata, span links, tracing IDs

Method categories:
- `methods-Encoding_and_Encryption.txt` - Base64, compression, hashing, encryption
- `methods-General.txt` - Basic operations, type checking
- `methods-GeoIP.txt` - GeoIP lookups
- `methods-JSON_Web_Tokens.txt` - JWT operations
- `methods-Number_Manipulation.txt` - Arithmetic, rounding, formatting
- `methods-Object___Array_Manipulation.txt` - Filtering, mapping, sorting, merging
- `methods-Parsing.txt` - JSON, CSV, XML, protocol buffer parsing
- `methods-Regular_Expressions.txt` - Regex matching and replacement
- `methods-SQL.txt` - SQL operations
- `methods-String_Manipulation.txt` - Case, trimming, splitting, formatting
- `methods-Timestamp_Manipulation.txt` - Parsing, formatting, timezone conversion
- `methods-Type_Coercion.txt` - Type conversions

**Output Format:**

Each category file contains XML-formatted entries:

```xml
<function name="counter" params="min:query expression, max:query expression, set:query expression">
Generates an incrementing sequence of integers starting from a minimum value (default 1).
Each counter instance maintains its own independent state across message processing.
When the maximum value is reached, the counter automatically resets to the minimum.
<example summary="Generate sequential IDs for each message.">
root.id = counter()
</example>
<example summary="Use a custom range for the counter.">
root.batch_num = counter(min: 100, max: 200)
</example>
</function>

<method name="ts_format" params="format:string, tz:string">
Formats a timestamp into a string using the specified format layout.
<example>
root.formatted = this.timestamp.ts_format("2006-01-02T15:04:05Z07:00")
</example>
</method>
```

**Format Details:**
- `name` attribute: Function/method name
- `params` attribute: Parameter list with types (empty string if no params)
- Description: Multi-line text (no prefix)
- `<example>` tags: Code blocks, optional `summary` attribute
- Error messages: Lines starting with "ERROR" indicate missing descriptions

**Quick Reference - Listing Available Functions/Methods:**

Search category files without loading full content:

```bash
# List all available functions and methods by name
grep -hE '<(function|method) name=' "$SESSION_DIR"/*.txt

# Search by keyword (searches names, descriptions, params, examples)
grep -i "timestamp" "$SESSION_DIR"/*.txt

# Search by parameter name (e.g., find all with "format" parameter)
grep 'params="[^"]*format' "$SESSION_DIR"/*.txt
```

This allows efficient discovery before reading full category files.

**test-blobl.sh**:
```bash
# Usage:
${CLAUDE_PLUGIN_ROOT}/scripts/test-blobl.sh <session-directory>
```
- Accepts session directory path as the only parameter
- Expects `data.json` (input) and `script.blobl` (transformation) in that directory
- Executes the Bloblang script and returns results or errors
- Can be run repeatedly during iteration

**Session directory lifecycle**:
- Created at conversation start: `SESSION_DIR=$(mktemp -d -t rpcn-blobl-XXXXXX)`
- Populated with category files: `format-bloblang.sh --output-dir "$SESSION_DIR"`
- Contains data.json and script.blobl for testing
- Cleaned up when conversation ends: `rm -rf "$SESSION_DIR"`

## Quality Standards

- **Never present untested code** - All scripts must be validated before showing to user
- **Use real functions** - Discover actual Bloblang functions from category files, don't guess
- **Provide working examples** - Include tested input/output pairs
- **Iterate on failures** - Debug errors until the script works correctly
- **Document assumptions** - Note any assumptions about input data structure or types
