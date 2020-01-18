---
title: parallel
type: processor
---

```yaml
parallel:
  cap: 0
  processors: []
```

A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message (similar to the
[`for_each`](for_each) processor), but where each message is
processed in parallel.

The field `cap`, if greater than zero, caps the maximum number of
parallel processing threads.


