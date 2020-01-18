---
title: redis_list
type: input
---

```yaml
redis_list:
  key: benthos_list
  timeout: 5s
  url: tcp://localhost:6379
```

Pops messages from the beginning of a Redis list using the BLPop command.


