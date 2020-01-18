---
title: for_each
type: processor
---

```yaml
for_each: []
```

A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message. This is useful for forcing batch
wide processors such as [`dedupe`](dedupe) or interpolations such as
the `value` field of the `metadata` processor to execute on
individual message parts of a batch instead.

Please note that most processors already process per message of a batch, and
this processor is not needed in those cases.


