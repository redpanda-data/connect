---
title: all
type: condition
---

```yaml
all: {}
```

All is a condition that tests a child condition against each message of a batch
individually. If all messages pass the child condition then this condition also
passes.

For example, if we wanted to check that all messages of a batch contain the word
'foo' we could use this config:

``` yaml
all:
  text:
    operator: contains
    arg: foo
```


