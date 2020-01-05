Rate Limits
===========

This document was generated with `benthos --list-rate-limits`

A rate limit is a strategy for limiting the usage of a shared resource across
parallel components in a Benthos instance, or potentially across multiple
instances.

For example, if we wanted to protect an HTTP service with a local rate limit
we could configure one like so:

``` yaml
input:
  type: foo
pipeline:
  threads: 8
  processors:
  - http:
      request:
        url: http://foo.bar/baz
        rate_limit: foobar
      parallel: true
resources:
  rate_limits:
    foobar:
      local:
        count: 500
        interval: 1s
```

In this example if the messages from the input `foo` are batches the
requests of a batch will be sent in parallel. This is usually going to be what
we want, but could potentially stress our HTTP server if a batch is large.

However, by using a rate limit we can guarantee that even across parallel
processing pipelines and variable sized batches we wont hit the service more
than 500 times per second.

### Contents

1. [`local`](#local)

## `local`

``` yaml
type: local
local:
  count: 1000
  interval: 1s
```

The local rate limit is a simple X every Y type rate limit that can be shared
across any number of components within the pipeline.

