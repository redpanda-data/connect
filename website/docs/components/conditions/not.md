---
title: not
type: condition
---

```yaml
not: {}
```

Not is a condition that returns the opposite (NOT) of its child condition. The
body of a not object is the child condition, i.e. in order to express 'part 0
NOT equal to "foo"' you could have the following YAML config:

``` yaml
not:
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


