---
title: Rate Limits
sidebar_label: About
---

A rate limit is a strategy for limiting the usage of a shared resource across parallel components in a Benthos instance, or potentially across multiple instances.

For example, if we wanted to protect an HTTP service with a local rate limit we could configure one like so:

```yaml
input:
  http_client:
    url: TODO
    rate_limit: foobar

resources:
  rate_limits:
    foobar:
      local:
        count: 500
        interval: 1s
```

By using a rate limit in this way we can guarantee that our input will only poll our HTTP source at the rate of 500 requests per second.