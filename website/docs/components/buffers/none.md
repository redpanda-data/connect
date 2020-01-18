---
title: none
type: buffer
---

```yaml
none: {}
```

Selecting no buffer (default) means the output layer is directly coupled with
the input layer. This is the safest and lowest latency option since
acknowledgements from at-least-once protocols can be propagated all the way from
the output protocol to the input protocol.

If the output layer is hit with back pressure it will propagate all the way to
the input layer, and further up the data stream. If you need to relieve your
pipeline of this back pressure consider using a more robust buffering solution
such as Kafka before resorting to alternatives.


