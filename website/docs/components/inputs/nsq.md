---
title: nsq
type: input
---

```yaml
input:
  nsq:
    nsqd_tcp_addresses:
    - localhost:4150
    lookupd_http_addresses:
    - localhost:4161
    topic: benthos_messages
    channel: benthos_stream
    user_agent: benthos_consumer
    max_in_flight: 100
```

Subscribe to an NSQ instance topic and channel.

## Fields

### `nsqd_tcp_addresses`

`array` A list of nsqd addresses to connect to.

### `lookupd_http_addresses`

`array` A list of nsqlookupd addresses to connect to.

### `topic`

`string` The topic to consume from.

### `channel`

`string` The channel to consume from.

### `user_agent`

`string` A user agent to assume when connecting.

### `max_in_flight`

`number` The maximum number of pending messages to consume at any given time.


