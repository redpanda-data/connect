---
title: rate_limit
type: processor
---

```yaml
rate_limit:
  resource: ""
```

Throttles the throughput of a pipeline according to a specified
[`rate_limit`](/docs/components/rate_limits/about) resource. Rate limits are
shared across components and therefore apply globally to all processing
pipelines.


