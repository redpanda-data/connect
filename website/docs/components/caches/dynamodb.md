---
title: dynamodb
type: cache
---

```yaml
dynamodb:
  backoff:
    initial_interval: 1s
    max_elapsed_time: 30s
    max_interval: 5s
  consistent_read: false
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  data_key: ""
  endpoint: ""
  hash_key: ""
  max_retries: 3
  region: eu-west-1
  table: ""
  ttl: ""
  ttl_key: ""
```

The dynamodb cache stores key/value pairs as a single document in a DynamoDB
table. The key is stored as a string value and used as the table hash key. The
value is stored as a binary value using the `data_key` field name.

A prefix can be specified to allow multiple cache types to share a single
DynamoDB table. An optional TTL duration (`ttl`) and field
(`ttl_key`) can be specified if the backing table has TTL enabled.

Strong read consistency can be enabled using the `consistent_read`
configuration field.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/aws).


