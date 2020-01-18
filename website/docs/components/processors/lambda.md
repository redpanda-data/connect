---
title: lambda
type: processor
---

```yaml
lambda:
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  endpoint: ""
  function: ""
  parallel: false
  rate_limit: ""
  region: eu-west-1
  retries: 3
  timeout: 5s
```

Invokes an AWS lambda for each message part of a batch. The contents of the
message part is the payload of the request, and the result of the invocation
will become the new contents of the message.

It is possible to perform requests per message of a batch in parallel by setting
the `parallel` flag to `true`. The `rate_limit`
field can be used to specify a rate limit [resource](/docs/components/rate_limits/about)
to cap the rate of requests across parallel components service wide.

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the [`process_map`](process_map) or
 [`process_field`](process_field) processors.

### Error Handling

When all retry attempts for a message are exhausted the processor cancels the
attempt. These failed messages will continue through the pipeline unchanged, but
can be dropped or placed in a dead letter queue according to your config, you
can read about these patterns [here](/docs/configuration/error_handling).

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).


