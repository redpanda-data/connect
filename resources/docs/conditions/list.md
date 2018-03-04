CONDITIONS
==========

This document has been generated with `benthos --list-conditions`.

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
	"type": "not"
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
