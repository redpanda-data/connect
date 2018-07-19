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
  type: and
  and:
  - type: text
    text:
      operator: contains
      arg: hello world
  - type: or
    or:
    - type: text
      text:
        operator: contains
        arg: foo
    - type: not
      not:
        type: text
        text:
          operator: contains
          arg: bar
```

The above example could be summarised as 'text contains "hello world" and also
either contains "foo" or does _not_ contain "bar"'.

Conditions can be extremely useful for creating filters on an output. By using a
fan out output broker with 'filter' processors on the brokered outputs it is
possible to build
[curated data streams](../concepts.md#content-based-multiplexing) that filter on
the content of each message.

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

1. [`and`](#and)
2. [`count`](#count)
3. [`jmespath`](#jmespath)
4. [`not`](#not)
5. [`or`](#or)
6. [`resource`](#resource)
7. [`static`](#static)
8. [`text`](#text)
9. [`xor`](#xor)

## `and`

``` yaml
type: and
and: []
```

And is a condition that returns the logical AND of its children conditions.

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

## `or`

``` yaml
type: or
or: []
```

Or is a condition that returns the logical OR of its children conditions.

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
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: foo
      foo:
        processors:
        - type: filter
          filter:
            type: resource
            resource: foobar
    - type: bar
      bar:
        processors:
        - type: filter
		  filter:
            type: not
            not:
              type: resource
              resource: foobar
resources:
  conditions:
    foobar:
      type: text
      text:
        operator: equals_cs
        part: 1
        arg: filter me please
```

It is also worth noting that when conditions are used as resources in this way
they will only be executed once per message, regardless of how many times they
are referenced (unless the content is modified). Therefore, resource conditions
can act as a runtime optimisation as well as a config optimisation.

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

Text is a condition that checks the contents of a message part as plain text
against a logical operator and an argument.

Available logical operators are:

### `equals_cs`

Checks whether the part equals the argument (case sensitive.)

### `equals`

Checks whether the part equals the argument under unicode case-folding (case
insensitive.)

### `contains_cs`

Checks whether the part contains the argument (case sensitive.)

### `contains`

Checks whether the part contains the argument under unicode case-folding (case
insensitive.)

### `prefix_cs`

Checks whether the part begins with the argument (case sensitive.)

### `prefix`

Checks whether the part begins with the argument under unicode case-folding
(case insensitive.)

### `suffix_cs`

Checks whether the part ends with the argument (case sensitive.)

### `suffix`

Checks whether the part ends with the argument under unicode case-folding (case
insensitive.)

### `regexp_partial`

Checks whether any section of the message part matches a regular expression (RE2
syntax).

### `regexp_exact`

Checks whether the message part exactly matches a regular expression (RE2
syntax).

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
