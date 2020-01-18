---
title: local
type: rate_limit
---

```yaml
local:
  count: 1000
  interval: 1s
```

The local rate limit is a simple X every Y type rate limit that can be shared
across any number of components within the pipeline.


