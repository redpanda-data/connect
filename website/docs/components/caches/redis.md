---
title: redis
type: cache
---

```yaml
redis:
  expiration: 24h
  prefix: ""
  retries: 3
  retry_period: 500ms
  url: tcp://localhost:6379
```

Use a Redis instance as a cache. The expiration can be set to zero or an empty
string in order to set no expiration.


