---
title: and
type: condition
---

```yaml
and: []
```

And is a condition that returns the logical AND of its children conditions:

``` yaml
# True if message contains both 'foo' and 'bar'
and:
  - text:
      operator: contains
      arg: foo
  - text:
      operator: contains
      arg: bar
```


