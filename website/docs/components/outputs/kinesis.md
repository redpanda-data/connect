---
title: kinesis
type: output
---

```yaml
kinesis:
  backoff:
    initial_interval: 1s
    max_elapsed_time: 30s
    max_interval: 5s
  batching:
    byte_size: 0
    condition:
      static: false
      type: static
    count: 1
    period: ""
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  endpoint: ""
  hash_key: ""
  max_in_flight: 1
  max_retries: 0
  partition_key: ""
  region: eu-west-1
  stream: ""
```

Sends messages to a Kinesis stream.

Both the `partition_key`(required) and `hash_key` (optional)
fields can be dynamically set using function interpolations described
[here](/docs/configuration/interpolation#functions). When sending batched messages the
interpolations are performed per message part.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).

This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field `max_in_flight`.

This output benefits from sending messages as a batch for improved performance.
Batches can be formed at both the input and output level. You can find out more
[in this doc](/docs/configuration/batching).


