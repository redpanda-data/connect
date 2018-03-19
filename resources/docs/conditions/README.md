CONDITIONS
==========

This document was generated with `benthos --list-conditions`

Within the list of Benthos [processors][0] you will find the [condition][1]
processor, which applies a condition to every message and only propagates them
if the condition passes. Conditions themselves can modify ('not') and combine
('and', 'or') other conditions, and can therefore be used to create complex
filters.

Conditions can be extremely useful for creating filters on an output. By using a
fan out output broker with 'condition' processors on the brokered outputs it is
possible to build curated data streams that filter on the content of each
message.

Here is an example config, where we have an output that receives only 'foo'
messages, and an output that receives only 'bar' messages, and a third output
that receives everything:

``` yaml
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
      - type: file
        file:
          path: ./foo.txt
        processors:
        - type: condition
          condition:
            type: content
            content:
              operator: contains
              part: 0
              arg: foo
      - type: file
        file:
          path: ./bar.txt
        processors:
        - type: condition
          condition:
            type: content
            content:
              operator: contains
              part: 0
              arg: bar
      - type: file
        file:
          path: ./everything.txt
```

Sometimes large chunks of logic are reused across processors, or nested multiple
times as branches of a larger condition. It is possible to avoid writing
duplicate condition configs by using the [resource condition][2].

## `and`

And is a condition that returns the logical AND of its children conditions.

## `content`

Content is a condition that checks the content of a message part against a
logical operator and an argument.

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

## `not`

Not is a condition that returns the opposite (NOT) of its child condition. The
body of a not object is the child condition, i.e. in order to express 'part 0
NOT equal to "foo"' you could have the following YAML config:

``` yaml
type: not
not:
  type: content
  content:
    operator: equal
    part: 0
    arg: foo
```

Or, the same example as JSON:

``` json
{
	"type": "not",
	"not": {
		"type": "content",
		"content": {
			"operator": "equal",
			"part": 0,
			"arg": "foo"
		}
	}
}
```

## `or`

Or is a condition that returns the logical OR of its children conditions.

## `resource`

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
        - type: condition
          condition:
            type: resource
            resource: foobar
    - type: bar
      bar:
        processors:
        - type: condition
          condition:
            type: not
            not:
              type: resource
              resource: foobar
resources:
  conditions:
    foobar:
      type: content
      content:
        operator: equals_cs
        part: 1
        arg: filter me please
```

It is also worth noting that when conditions are used as resources in this way
they will only be executed once per message, regardless of how many times they
are referenced (unless the content is modified). Therefore, resource conditions
can act as a runtime optimisation as well as a config optimisation.

[0]: ../processors/README.md
[1]: ../processors/README.md#condition
[2]: #resource
