---
title: memory
type: buffer
---

```yaml
memory:
  batch_policy:
    byte_size: 0
    condition:
      static: false
      type: static
    count: 0
    enabled: false
    period: ""
  limit: 524288000
```

The memory buffer stores messages in RAM. During shutdown Benthos will make a
best attempt at flushing all remaining messages before exiting cleanly.

This buffer has a configurable limit, where consumption will be stopped with
back pressure upstream if the total size of messages in the buffer reaches this
amount. Since this calculation is only an estimate, and the real size of
messages in RAM is always higher, it is recommended to set the limit
significantly below the amount of RAM available.

### Batching

It is possible to batch up messages sent from this buffer using a
[batch policy](/docs/configuration/batching#batch-policy).

This is a more powerful way of batching messages than the
[`batch`](/docs/components/processors/batch) processor, as it does not
rely on new messages entering the pipeline in order to trigger the conditions.


