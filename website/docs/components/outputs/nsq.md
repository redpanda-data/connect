---
title: nsq
type: output
---

```yaml
nsq:
  max_in_flight: 1
  nsqd_tcp_address: localhost:4150
  topic: benthos_messages
  user_agent: benthos_producer
```

Publish to an NSQ topic. The `topic` field can be dynamically set
using function interpolations described
[here](/docs/configuration/interpolation#functions). When sending batched messages
these interpolations are performed per message part.

This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field `max_in_flight`.


