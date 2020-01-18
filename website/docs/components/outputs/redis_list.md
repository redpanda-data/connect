---
title: redis_list
type: output
---

```yaml
redis_list:
  key: benthos_list
  max_in_flight: 1
  url: tcp://localhost:6379
```

Pushes messages onto the end of a Redis list (which is created if it doesn't
already exist) using the RPUSH command.

This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages with the
field `max_in_flight`.


