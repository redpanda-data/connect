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

[0]: ../processors/README.md
[1]: ../processors/README.md#condition
