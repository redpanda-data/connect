---
title: nats_stream
type: output
---

```yaml
nats_stream:
  client_id: benthos_client
  cluster_id: test-cluster
  max_in_flight: 1
  subject: benthos_messages
  urls:
  - nats://127.0.0.1:4222
```

Publish to a NATS Stream subject.

This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field `max_in_flight`.


