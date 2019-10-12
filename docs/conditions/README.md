Conditions
==========

This document was generated with `benthos --list-conditions`

Conditions are boolean queries that can be executed based on the contents of a
message. Some [processors][processors] such as [`filter`][filter] use
conditions for expressing their logic.

Conditions themselves can modify (`not`) and combine (`and`, `or`)
other conditions, and can therefore be used to create complex boolean
expressions.

The format of a condition is similar to other Benthos types:

``` yaml
condition:
  type: text
  text:
    operator: equals
    part: 0
    arg: hello world
```

And using boolean condition types we can combine multiple conditions together:

``` yaml
condition:
  and:
  - text:
      operator: contains
      arg: hello world
  - or:
    - text:
        operator: contains
        arg: foo
    - not:
        text:
          operator: contains
          arg: bar
```

The above example could be summarised as 'text contains "hello world" and also
either contains "foo" or does _not_ contain "bar"'.

### Batching and Multipart Messages

All conditions can be applied to a multipart message, which is synonymous with a
batch. Some conditions target a specific part of a message batch, and require
you specify the target index with the field `part`.

Some processors such as [`filter`][filter] apply its conditions across
the whole batch. Whereas other processors such as
 [`filter_parts`][filter_parts] will apply its conditions on each part
of a batch individually, in which case the condition acts as if it were
referencing a single message batch.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the last
element with be selected, and so on.

### Reusing Conditions

Sometimes large chunks of logic are reused across processors, or nested multiple
times as branches of a larger condition. It is possible to avoid writing
duplicate condition configs by using the [resource condition][resource].

### Contents

1. [`all`](#all)
2. [`and`](#and)
3. [`any`](#any)
4. [`bounds_check`](#bounds_check)
5. [`check_field`](#check_field)
6. [`check_interpolation`](#check_interpolation)
7. [`count`](#count)
8. [`jmespath`](#jmespath)
9. [`json_schema`](#json_schema)
10. [`metadata`](#metadata)
11. [`not`](#not)
12. [`number`](#number)
13. [`or`](#or)
14. [`processor_failed`](#processor_failed)
15. [`resource`](#resource)
16. [`static`](#static)
17. [`text`](#text)
18. [`xor`](#xor)

## `all`

``` yaml
type: all
all: {}
```

All is a condition that tests a child condition against each message of a batch
individually. If all messages pass the child condition then this condition also
passes.

For example, if we wanted to check that all messages of a batch contain the word
'foo' we could use this config:

``` yaml
type: all
all:
  type: text
  text:
    operator: contains
    arg: foo
```

## `and`

``` yaml
type: and
and: []
```

And is a condition that returns the logical AND of its children conditions.

## `any`

``` yaml
type: any
any: {}
```

Any is a condition that tests a child condition against each message of a batch
individually. If any message passes the child condition then this condition also
passes.

For example, if we wanted to check that at least one message of a batch contains
the word 'foo' we could use this config:

``` yaml
any:
  text:
    operator: contains
    arg: foo
```

## `bounds_check`

``` yaml
type: bounds_check
bounds_check:
  max_part_size: 1.073741824e+09
  max_parts: 100
  min_part_size: 1
  min_parts: 1
```

Checks a message against a set of bounds.

## `check_field`

``` yaml
type: check_field
check_field:
  condition: {}
  parts: []
  path: ""
```

Extracts the value of a field identified via [dot path](../field_paths.md)
within messages (currently only JSON format is supported) and then tests the
extracted value against a child condition.

## `check_interpolation`

``` yaml
type: check_interpolation
check_interpolation:
  condition: {}
  value: ""
```

Resolves a string containing
[function interpolations](../config_interpolation.md#functions) and then tests
the result against a child condition.

For example, you could use this to test against the size of a message batch:

``` yaml
check_interpolation:
  value: ${!batch_size}
  condition:
    number:
      operator: greater_than
      arg: 1
```

## `count`

``` yaml
type: count
count:
  arg: 100
```

Counts messages starting from one, returning true until the counter reaches its
target, at which point it will return false and reset the counter. This
condition is useful when paired with the `read_until` input, as it can
be used to cut the input stream off once a certain number of messages have been
read.

It is worth noting that each discrete count condition will have its own counter.
Parallel processors containing a count condition will therefore count
independently. It is, however, possible to share the counter across processor
pipelines by defining the count condition as a resource.

## `jmespath`

``` yaml
type: jmespath
jmespath:
  part: 0
  query: ""
```

Parses a message part as a JSON blob and attempts to apply a JMESPath expression
to it, expecting a boolean response. If the response is true the condition
passes, otherwise it does not. Please refer to the
[JMESPath website](http://jmespath.org/) for information and tutorials regarding
the syntax of expressions.

For example, with the following config:

``` yaml
jmespath:
  part: 0
  query: a == 'foo'
```

If the initial jmespaths of part 0 were:

``` json
{
	"a": "foo"
}
```

Then the condition would pass.

JMESPath is traditionally used for mutating JSON, in order to do this please
instead use the [`jmespath`](../processors/README.md#jmespath)
processor.

## `json_schema`

``` yaml
type: json_schema
json_schema:
  part: 0
  schema: ""
  schema_path: ""
```

Validates a message against the provided JSONSchema definition to retrieve a
boolean response indicating whether the message matches the schema or not.
If the response is true the condition passes, otherwise it does not. Please
refer to the [JSON Schema website](https://json-schema.org/) for information and
tutorials regarding the syntax of the schema.

For example, with the following JSONSchema document:

``` json
{
	"$id": "https://example.com/person.schema.json",
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title": "Person",
	"type": "object",
	"properties": {
	  "firstName": {
		"type": "string",
		"description": "The person's first name."
	  },
	  "lastName": {
		"type": "string",
		"description": "The person's last name."
	  },
	  "age": {
		"description": "Age in years which must be equal to or greater than zero.",
		"type": "integer",
		"minimum": 0
	  }
	}
}
```

And the following Benthos configuration:

``` yaml
json_schema:
  part: 0
  schema_path: "file://path_to_schema.json"
```

If the message being processed looked like:

``` json
{"firstName":"John","lastName":"Doe","age":21}
```

Then the condition would pass.

## `metadata`

``` yaml
type: metadata
metadata:
  arg: ""
  key: ""
  operator: equals_cs
  part: 0
```

Metadata is a condition that checks metadata keys of a message part against an
operator from the following list:

### `enum`

Checks whether the contents of a metadata key matches one of the defined enum
values.

```yaml
metadata:
  operator: enum
  part: 0
  key: foo
  arg:
    - bar
    - baz
    - qux
    - quux
```

### `equals`

Checks whether the contents of a metadata key matches an argument. This operator
is case insensitive.

```yaml
metadata:
  operator: equals
  part: 0
  key: foo
  arg: bar
```

### `equals_cs`

Checks whether the contents of a metadata key matches an argument. This operator
is case sensitive.

```yaml
metadata:
  operator: equals_cs
  part: 0
  key: foo
  arg: BAR
```

### `exists`

Checks whether a metadata key exists.

```yaml
metadata:
  operator: exists
  part: 0
  key: foo
```

### `greater_than`

Checks whether the contents of a metadata key, parsed as a floating point
number, is greater than an argument. Returns false if the metadata value cannot
be parsed into a number.

```yaml
metadata:
  operator: greater_than
  part: 0
  key: foo
  arg: 3
```

### `has_prefix`

Checks whether the contents of a metadata key match one of the provided prefixes.
The arg field can either be a singular prefix string or a list of prefixes.

```yaml
metadata:
  operator: has_prefix
  part: 0
  key: foo
  arg:
    - foo
    - bar
    - baz
```

### `less_than`

Checks whether the contents of a metadata key, parsed as a floating point
number, is less than an argument. Returns false if the metadata value cannot be
parsed into a number.

```yaml
metadata:
  operator: less_than
  part: 0
  key: foo
  arg: 3
```

### `regexp_partial`

Checks whether any section of the contents of a metadata key matches a regular
expression (RE2 syntax).

```yaml
metadata:
  operator: regexp_partial
  part: 0
  key: foo
  arg: "1[a-z]2"
```

### `regexp_exact`

Checks whether the contents of a metadata key exactly matches a regular expression 
(RE2 syntax).

```yaml
metadata:
  operator: regexp_partial
  part: 0
  key: foo
  arg: "1[a-z]2"
```


## `not`

``` yaml
type: not
not: {}
```

Not is a condition that returns the opposite (NOT) of its child condition. The
body of a not object is the child condition, i.e. in order to express 'part 0
NOT equal to "foo"' you could have the following YAML config:

``` yaml
type: not
not:
  type: text
  text:
    operator: equal
    part: 0
    arg: foo
```

Or, the same example as JSON:

``` json
{
	"type": "not",
	"not": {
		"type": "text",
		"text": {
			"operator": "equal",
			"part": 0,
			"arg": "foo"
		}
	}
}
```

## `number`

``` yaml
type: number
number:
  arg: 0
  operator: equals
  part: 0
```

Number is a condition that checks the contents of a message parsed as a 64-bit
floating point number against a logical operator and an argument.

It's possible to use the [`check_field`](#check_field) and
[`check_interpolation`](#check_interpolation) conditions to check a
number condition against arbitrary metadata or fields of messages. For example,
you can test a number condition against the size of a message batch with:

``` yaml
check_interpolation:
  value: ${!batch_size}
  condition:
    number:
      operator: greater_than
      arg: 1
```

Available logical operators are:

### `equals`

Checks whether the value equals the argument.

### `greater_than`

Checks whether the value is greater than the argument. Returns false if the
value cannot be parsed as a number.

### `less_than`

Checks whether the value is less than the argument. Returns false if the value
cannot be parsed as a number.

## `or`

``` yaml
type: or
or: []
```

Or is a condition that returns the logical OR of its children conditions.

## `processor_failed`

``` yaml
type: processor_failed
processor_failed:
  part: 0
```

Returns true if a processing stage of a message has failed. This condition is
useful for dropping failed messages or creating dead letter queues, you can read
more about these patterns [here](../error_handling.md).

## `resource`

``` yaml
type: resource
resource: ""
```

Resource is a condition type that runs a condition resource by its name. This
condition allows you to run the same configured condition resource in multiple
processors, or as a branch of another condition.

For example, let's imagine we have two outputs, one of which only receives
messages that satisfy a condition and the other receives the logical NOT of that
same condition. In this example we can save ourselves the trouble of configuring
the same condition twice by referring to it as a resource, like this:

``` yaml
output:
  broker:
    pattern: fan_out
    outputs:
    - foo:
        processors:
        - filter:
            type: resource
            resource: foobar
    - bar:
        processors:
        - filter:
            not:
              type: resource
              resource: foobar
resources:
  conditions:
    foobar:
      text:
        operator: equals_cs
        part: 1
        arg: filter me please
```

## `static`

``` yaml
type: static
static: true
```

Static is a condition that always resolves to the same static boolean value.

## `text`

``` yaml
type: text
text:
  arg: ""
  operator: equals_cs
  part: 0
```

Text is a condition that checks the contents of a message as plain text against
a logical operator and an argument.

It's possible to use the [`check_field`](#check_field) and
[`check_interpolation`](#check_interpolation) conditions to check a
text condition against arbitrary metadata or fields of messages. For example,
you can test a text condition against a JSON field `foo.bar` with:

``` yaml
check_field:
  path: foo.bar
  condition:
    text:
      operator: enum
      arg:
      - foo
      - bar
```

Available logical operators are:

### `equals_cs`

Checks whether the content equals the argument (case sensitive.)

### `equals`

Checks whether the content equals the argument under unicode case-folding (case
insensitive.)

### `contains_cs`

Checks whether the content contains the argument (case sensitive.)

### `contains`

Checks whether the content contains the argument under unicode case-folding
(case insensitive.)

### `is`

Checks whether the content meets the characteristic of a type specified in 
the argument field. Supported types are `ip`, `ipv4`, `ipv6`.

### `prefix_cs`

Checks whether the content begins with the argument (case sensitive.)

### `prefix`

Checks whether the content begins with the argument under unicode case-folding
(case insensitive.)

### `suffix_cs`

Checks whether the content ends with the argument (case sensitive.)

### `suffix`

Checks whether the content ends with the argument under unicode case-folding
(case insensitive.)

### `regexp_partial`

Checks whether any section of the content matches a regular expression (RE2
syntax).

### `regexp_exact`

Checks whether the content exactly matches a regular expression (RE2 syntax).

### `enum`

Checks whether the content matches any entry of a list of arguments, the field
`arg` must be an array for this operator, e.g.:

``` yaml
text:
  operator: enum
  arg:
  - foo
  - bar
```

## `xor`

``` yaml
type: xor
xor: []
```

Xor is a condition that returns the logical XOR of its children conditions,
meaning it only resolves to true if _exactly_ one of its children conditions
resolves to true.

[processors]: ../processors/README.md
[filter]: ../processors/README.md#filter
[filter_parts]: ../processors/README.md#filter_parts
[resource]: #resource
