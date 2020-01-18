---
title: or
type: condition
---

```yaml
or: []
```

Or is a condition that returns the logical OR of its children conditions.

``` yaml
# True if message contains 'foo' or 'bar'
or:
  - text:
      operator: contains
      arg: foo
  - text:
      operator: contains
      arg: bar
```


