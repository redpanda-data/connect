---
title: filter_parts
type: processor
---

```yaml
filter_parts:
  text:
    arg: ""
    operator: equals_cs
    part: 0
  type: text
```

Tests each individual message of a batch against a condition, if the condition
fails then the message is dropped. If the resulting batch is empty it will be
dropped. You can find a [full list of conditions here](/docs/components/conditions/about), in this
case each condition will be applied to a message as if it were a single message
batch.

This processor is useful if you are combining messages into batches using the
[`batch`](batch) processor and wish to remove specific parts.


